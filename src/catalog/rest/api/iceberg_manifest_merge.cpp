#include "catalog/rest/api/iceberg_manifest_merge.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/logging/logger.hpp"

#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

#include "yyjson.hpp"

#include <algorithm>
#include <string>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Config
//===--------------------------------------------------------------------===//
namespace {

constexpr const char *MERGE_ENABLED = "commit.manifest-merge.enabled";
constexpr const char *MIN_COUNT_TO_MERGE = "commit.manifest.min-count-to-merge";
constexpr const char *TARGET_SIZE_BYTES = "commit.manifest.target-size-bytes";

//! Aligned with Apache Iceberg Java defaults (the authoritative reference).
constexpr bool MERGE_ENABLED_DEFAULT = true;
constexpr idx_t MIN_COUNT_TO_MERGE_DEFAULT = 100;
constexpr int64_t TARGET_SIZE_BYTES_DEFAULT = 8 * 1024 * 1024;

bool ParseBoolProperty(ClientContext &context, const char *property_name, const string &value, bool fallback) {
	if (value.empty()) {
		return fallback;
	}
	auto lowered = StringUtil::Lower(value);
	if (lowered == "true") {
		return true;
	}
	if (lowered == "false") {
		return false;
	}
	DUCKDB_LOG_WARNING(context,
	                   "Invalid value '%s' for Iceberg table property '%s'; expected true or false, using default "
	                   "value '%s'.",
	                   value, property_name, fallback ? "true" : "false");
	return fallback;
}

template <class T>
T ParseIntProperty(ClientContext &context, const char *property_name, const string &value, T fallback) {
	if (value.empty()) {
		return fallback;
	}
	try {
		auto parsed = std::stoll(value);
		//! A non-positive threshold is meaningless; fall back rather than disable merging silently.
		if (parsed > 0) {
			return static_cast<T>(parsed);
		}
	} catch (...) {
	}
	DUCKDB_LOG_WARNING(context,
	                   "Invalid value '%s' for Iceberg table property '%s'; expected a positive integer, using "
	                   "default value '%s'.",
	                   value, property_name, std::to_string(fallback));
	return fallback;
}

} // namespace

IcebergManifestMergeConfig IcebergManifestMergeConfig::FromTableMetadata(const IcebergTableMetadata &metadata,
                                                                         ClientContext &context) {
	IcebergManifestMergeConfig config;
	config.enabled =
	    ParseBoolProperty(context, MERGE_ENABLED, metadata.GetTableProperty(MERGE_ENABLED), MERGE_ENABLED_DEFAULT);
	config.min_count_to_merge = ParseIntProperty<idx_t>(
	    context, MIN_COUNT_TO_MERGE, metadata.GetTableProperty(MIN_COUNT_TO_MERGE), MIN_COUNT_TO_MERGE_DEFAULT);
	config.target_size_bytes = ParseIntProperty<int64_t>(
	    context, TARGET_SIZE_BYTES, metadata.GetTableProperty(TARGET_SIZE_BYTES), TARGET_SIZE_BYTES_DEFAULT);
	return config;
}

//===--------------------------------------------------------------------===//
// Bin-packing (pure logic)
//===--------------------------------------------------------------------===//
vector<vector<idx_t>> IcebergManifestMerge::BinPackManifests(const vector<int64_t> &weights, int64_t target_weight) {
	//! Mirror Java `ManifestMergeManager`/PyIceberg `ListPacker.pack_end` exactly: first-fit
	//! bin-packing with lookback=1 over the reversed input, then reverse the result. lookback=1 keeps
	//! at most one open bin, so manifests are not reordered across distant positions -- this is what
	//! makes the under-filled bin land on the newest manifests (merged next time) and avoids random
	//! deletes when data files are later aged off (see Java mergeGroup comment).
	//!
	//! PackingIterator semantics: maintain a list of open bins; place an item into the first open bin
	//! that fits, else open a new bin; whenever the number of open bins exceeds `lookback`, close
	//! (emit) the oldest open bin (FIFO). At the end, emit the remaining open bins in order.
	constexpr idx_t LOOKBACK = 1;

	struct OpenBin {
		vector<idx_t> items;
		int64_t weight = 0;
	};

	//! Pack over the reversed input (pack_end).
	vector<vector<idx_t>> packed; // closed bins, in close order
	vector<OpenBin> open_bins;
	for (idx_t rev = weights.size(); rev-- > 0;) {
		auto weight = weights[rev];
		OpenBin *target = nullptr;
		for (auto &b : open_bins) {
			if (b.weight + weight <= target_weight) {
				target = &b;
				break;
			}
		}
		if (target) {
			target->items.push_back(rev);
			target->weight += weight;
		} else {
			OpenBin b;
			b.items.push_back(rev);
			b.weight = weight;
			open_bins.push_back(std::move(b));
			if (open_bins.size() > LOOKBACK) {
				//! Close the oldest open bin (FIFO).
				packed.push_back(std::move(open_bins.front().items));
				open_bins.erase(open_bins.begin());
			}
		}
	}
	for (auto &b : open_bins) {
		packed.push_back(std::move(b.items));
	}

	//! pack_end: reverse each bin's items and the list of bins to restore original ascending order.
	vector<vector<idx_t>> result;
	result.reserve(packed.size());
	for (idx_t i = packed.size(); i-- > 0;) {
		auto &bin = packed[i];
		std::reverse(bin.begin(), bin.end());
		result.push_back(std::move(bin));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Merge decision
//===--------------------------------------------------------------------===//
bool IcebergManifestMerge::ShouldMergeBin(const vector<idx_t> &bin, idx_t min_count_to_merge) {
	if (bin.size() <= 1) {
		return false;
	}
	//! Only merge a bin once it has accumulated at least `min-count-to-merge` manifests (Apache
	//! Iceberg's ManifestMergeManager semantics). A first-fit bin can under-fill (its manifests do
	//! not reach the target size), and such a bin must not be rewritten until enough small manifests
	//! pile up -- otherwise every commit rewrites a handful of manifests, churning metadata and
	//! changing the manifest layout that read-time pruning relies on.
	if (bin.size() < min_count_to_merge) {
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Merge execution
//===--------------------------------------------------------------------===//

//! Read the manifest_entries of a manifest from its Avro file, reusing the vectorized manifest
//! reader. Returns the list entry with `manifest_entries` populated. Shared by the delete-rewrite
//! path and the merge path so both load entries identically.
IcebergManifestListEntry IcebergManifestMerge::ScanManifestEntries(const IcebergManifestListEntry &list_entry,
                                                                   IcebergCommitState &commit_state,
                                                                   int32_t schema_id) {
	vector<IcebergManifestListEntry> manifest_files;
	manifest_files.push_back(list_entry);
	manifest_files[0].manifest_entries.reset();

	IcebergOptions options;
	auto &fs = FileSystem::GetFileSystem(commit_state.context);
	auto &table_metadata = commit_state.table_info.table_metadata;

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = commit_state.latest_snapshot;
	snapshot_info.schema_id = schema_id;

	auto manifest_scan =
	    AvroScan::ScanManifest(snapshot_info, manifest_files, options, fs, "", table_metadata, commit_state.context);
	auto reader = make_uniq<manifest_file::ManifestReader>(*manifest_scan);
	while (!reader->Finished()) {
		reader->Read();
	}
	return std::move(manifest_files[0]);
}

IcebergManifestListEntry IcebergManifestMerge::WriteReplacementManifest(
    const IcebergManifestMetadata &manifest_metadata, vector<IcebergManifestEntry> &&manifest_entries,
    CopyFunction &avro_copy, DatabaseInstance &db, IcebergCommitState &commit_state,
    optional<sequence_number_t> first_row_id, optional<sequence_number_t> min_sequence_number) {
	auto &table_metadata = commit_state.table_info.table_metadata;
	int64_t scratch_row_id = 0;
	auto result = IcebergManifestListEntry::CreateFromEntries(FileSystem::GetFileSystem(commit_state.context),
	                                                          /*sequence_number*/ 0, table_metadata, manifest_metadata,
	                                                          std::move(manifest_entries), scratch_row_id);
	result.file.first_row_id = first_row_id;
	result.file.min_sequence_number = min_sequence_number;

	auto manifest_length = manifest_file::WriteToFile(table_metadata, result, avro_copy, db, commit_state.context);
	result.file.manifest_length = manifest_length;
	commit_state.created_metadata_files.push_back(result.file.manifest_path);
	return result;
}

namespace {

//! Merge one spec-homogeneous bin into a single new manifest. Returns the new list entry.
optional<IcebergManifestListEntry> MergeBin(const vector<IcebergManifestListEntry> &input, const vector<idx_t> &bin,
                                            IcebergManifestContentType content, CopyFunction &avro_copy,
                                            DatabaseInstance &db, IcebergCommitState &commit_state, int32_t schema_id,
                                            int32_t partition_spec_id) {
	auto &table_metadata = commit_state.table_info.table_metadata;
	const bool is_v3 = table_metadata.iceberg_version >= 3;

	//! Gather every entry from the bin's manifests, preserving status and historical sequence numbers.
	vector<IcebergManifestEntry> merged_entries;
	//! Merging is a pure physical repack: it creates no new rows, so V3 row lineage must be
	//! preserved, not reassigned. The first_row_id is a manifest-file-level value, so the merged
	//! manifest's first_row_id is the smallest first_row_id among the manifests it absorbs.
	//! This uses file-level metadata only -- no entry read required. Carried-over manifests always
	//! have a first_row_id by this point: a V2->V3 upgraded snapshot assigns one to every existing
	//! DATA manifest earlier in the commit (see IcebergTransactionData's upgrade handling), and new
	//! V3 data manifests are excluded from merging (they inherit their id only at write time).
	optional<int64_t> min_first_row_id;
	//! The merged manifest's min_sequence_number must be the smallest data_sequence_number among its
	//! entries. CreateFromEntries cannot compute this (it derives it from the manifest-file sequence
	//! number, which is a placeholder here), so we track the true minimum and set it ourselves before
	//! the manifest is written / handed to AddNewManifestFile. If left as the placeholder, scan
	//! planning's `seq > X` pruning would mis-judge which historical data the manifest can contain.
	optional<int64_t> min_seq;
	for (auto idx : bin) {
		auto &member = input[idx];
		if (is_v3 && member.file.first_row_id.has_value()) {
			if (!min_first_row_id || *member.file.first_row_id < *min_first_row_id) {
				min_first_row_id = *member.file.first_row_id;
			}
		}
		auto loaded = member.HasManifestEntries()
		                  ? member
		                  : IcebergManifestMerge::ScanManifestEntries(member, commit_state, schema_id);
		auto &loaded_entries = loaded.GetManifestEntries();
		//! V3 row lineage: a data file's _row_id is derived from its data_file.first_row_id (+ row
		//! position). That id is normally left null on disk and inherited at read time from the
		//! manifest's first_row_id plus the record_count of preceding files that also lack one. Merging
		//! rewrites these entries into a NEW manifest whose first_row_id is the min of the sources, so
		//! the inherited ids would be re-derived against that new base. The Iceberg spec requires the
		//! inherited value to be materialized into the data file metadata when creating existing/deleted
		//! entries, so we compute each entry's first_row_id here -- against THIS source manifest's
		//! first_row_id -- before the entries are moved into the merged manifest. That makes the merged
		//! row ids independent of the merged layout. (DELETE manifests carry no first_row_id, so this
		//! only applies to DATA content.)
		//!
		//! Note: with today's ordering invariants (carried-over manifests are prepended in first_row_id
		//! order and are contiguous), reading back would recompute the same ids even without this step;
		//! materializing is spec-compliant and keeps correctness independent of those invariants.
		if (is_v3 && content == IcebergManifestContentType::DATA && member.file.first_row_id.has_value()) {
			int64_t inherited_row_id = *member.file.first_row_id;
			for (auto &entry : loaded_entries) {
				if (!entry.data_file.HasFirstRowId()) {
					entry.data_file.SetFirstRowId(inherited_row_id);
					inherited_row_id += entry.data_file.record_count;
				}
			}
		}
		for (auto &entry : loaded_entries) {
			//! These are already-committed manifests. Any ADDED entry describes a file added by an
			//! earlier snapshot and must be materialized as EXISTING in the replacement manifest.
			if (entry.status == IcebergManifestEntryStatusType::ADDED) {
				auto seq = entry.GetSequenceNumber(member.file);
				auto file_seq = entry.GetFileSequenceNumber(member.file);
				entry.SetSequenceNumber(seq);
				entry.SetFileSequenceNumber(file_seq);
				entry.status = IcebergManifestEntryStatusType::EXISTING;
			}
			//! Live entries determine the manifest's minimum data sequence number.
			auto entry_seq = entry.GetSequenceNumber(member.file);
			if (!min_seq || entry_seq < *min_seq) {
				min_seq = entry_seq;
			}
			merged_entries.push_back(std::move(entry));
		}
	}

	//! A bin can collapse to nothing (e.g. every entry was a DELETED-by-an-earlier-snapshot entry and
	//! was filtered out above). An empty manifest must never be written -- WriteToFile asserts on it
	//! (and an empty Avro manifest is meaningless). Return an entry with no manifest_entries so the
	//! caller drops it; do this BEFORE CreateFromEntries/WriteToFile.
	if (merged_entries.empty()) {
		return std::nullopt;
	}

	//! CreateFromEntries computes all counts, min_sequence_number and the partition field summary
	//! from the entries; pass the bin's own spec id so the summary is computed against the correct
	//! (possibly historical) spec, not the table default.
	auto manifest_metadata = IcebergManifestMetadata(schema_id, partition_spec_id,
	                                                 NumericCast<int32_t>(table_metadata.iceberg_version), content);
	auto first_row_id =
	    is_v3 && content == IcebergManifestContentType::DATA && min_first_row_id ? min_first_row_id : nullopt;
	//! Set the true minimum data sequence number from the absorbed entries (see above). Done before
	//! WriteToFile / AddNewManifestFile so scan-planning pruning sees the correct lower bound.
	return IcebergManifestMerge::WriteReplacementManifest(manifest_metadata, std::move(merged_entries), avro_copy, db,
	                                                      commit_state, first_row_id, min_seq);
}

} // namespace

vector<IcebergManifestListEntry> IcebergManifestMerge::MergeManifests(vector<IcebergManifestListEntry> &&input,
                                                                      IcebergManifestContentType content,
                                                                      const IcebergManifestMergeConfig &config,
                                                                      CopyFunction &avro_copy, DatabaseInstance &db,
                                                                      IcebergCommitState &commit_state,
                                                                      int32_t current_schema_id) {
	vector<IcebergManifestListEntry> result;
	if (!config.enabled || input.size() <= 1) {
		for (auto &member : input) {
			result.push_back(std::move(member));
		}
		return result;
	}

	//! Load the entries of the manifests if they're not already loaded
	for (auto &member : input) {
		if (member.HasManifestEntries()) {
			//! Already loaded, no need to scan
			continue;
		}
		auto loaded = IcebergManifestMerge::ScanManifestEntries(member, commit_state, current_schema_id);
		member.file = std::move(loaded.file);
		member.manifest_entries = std::move(loaded.manifest_entries);
		member.manifest_metadata.emplace(*loaded.manifest_metadata);
	}

	//! Group by spec_id+schema_id, so we only merge manifests that are compatible
	map<std::pair<int32_t, int32_t>, vector<idx_t>> groups;
	for (idx_t i = 0; i < input.size(); i++) {
		auto schema_id = input[i].manifest_metadata->schema_id;
		auto spec_id = input[i].file.partition_spec_id;
		groups[std::make_pair(schema_id, spec_id)].push_back(i);
	}

	for (auto &group : groups) {
		auto schema_id = group.first.first;
		auto spec_id = group.first.second;
		auto &group_indices = group.second;

		//! Bin-pack using manifest-file-level lengths only. Indices in `bins` are into
		//! `group_indices`.
		vector<int64_t> weights;
		weights.reserve(group_indices.size());
		for (auto idx : group_indices) {
			weights.push_back(input[idx].file.manifest_length);
		}
		auto bins = IcebergManifestMerge::BinPackManifests(weights, config.target_size_bytes);

		for (auto &local_bin : bins) {
			//! Translate local (group) indices back to global input indices.
			vector<idx_t> bin;
			bin.reserve(local_bin.size());
			for (auto local : local_bin) {
				bin.push_back(group_indices[local]);
			}

			if (!IcebergManifestMerge::ShouldMergeBin(bin, config.min_count_to_merge)) {
				for (auto idx : bin) {
					result.push_back(std::move(input[idx]));
				}
				continue;
			}

			auto merged = MergeBin(input, bin, content, avro_copy, db, commit_state, schema_id, spec_id);
			//! A bin can collapse to nothing (e.g. all entries were deleted and filtered out); never
			//! write or reference an empty manifest.
			if (!merged) {
				continue;
			}
			result.push_back(std::move(*merged));
		}
	}
	return result;
}

void IcebergManifestMerge::MergeManifestList(vector<IcebergManifestListEntry> &manifests, int32_t current_schema_id,
                                             CopyFunction &avro_copy, DatabaseInstance &db,
                                             IcebergCommitState &commit_state) {
	auto config =
	    IcebergManifestMergeConfig::FromTableMetadata(commit_state.table_info.table_metadata, commit_state.context);
	if (!config.enabled) {
		return;
	}

	if (manifests.size() <= 1) {
		return;
	}

	vector<IcebergManifestListEntry> data_input;
	vector<IcebergManifestListEntry> delete_input;
	for (auto &entry : manifests) {
		auto &target = entry.file.content == IcebergManifestContentType::DELETE ? delete_input : data_input;
		target.push_back(std::move(entry));
	}

	auto merged_data = IcebergManifestMerge::MergeManifests(std::move(data_input), IcebergManifestContentType::DATA,
	                                                        config, avro_copy, db, commit_state, current_schema_id);
	auto merged_delete =
	    IcebergManifestMerge::MergeManifests(std::move(delete_input), IcebergManifestContentType::DELETE, config,
	                                         avro_copy, db, commit_state, current_schema_id);

	manifests.clear();
	for (auto &entry : merged_data) {
		manifests.push_back(std::move(entry));
	}
	for (auto &entry : merged_delete) {
		manifests.push_back(std::move(entry));
	}
}

} // namespace duckdb
