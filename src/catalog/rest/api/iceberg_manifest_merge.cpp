#include "catalog/rest/api/iceberg_manifest_merge.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/map.hpp"

#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

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

//! Aligned with Apache Iceberg Java defaults (the authoritative reference; see design doc 2.1).
constexpr bool MERGE_ENABLED_DEFAULT = true;
constexpr idx_t MIN_COUNT_TO_MERGE_DEFAULT = 100;
constexpr int64_t TARGET_SIZE_BYTES_DEFAULT = 8 * 1024 * 1024;

bool ParseBoolProperty(const string &value, bool fallback) {
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
	return fallback;
}

template <class T>
T ParseIntProperty(const string &value, T fallback) {
	if (value.empty()) {
		return fallback;
	}
	try {
		auto parsed = std::stoll(value);
		//! A non-positive threshold is meaningless; fall back rather than disable merging silently.
		return parsed > 0 ? static_cast<T>(parsed) : fallback;
	} catch (...) {
		return fallback;
	}
}

} // namespace

ManifestMergeConfig ManifestMergeConfig::FromTableMetadata(const IcebergTableMetadata &metadata) {
	ManifestMergeConfig config;
	config.enabled = ParseBoolProperty(metadata.GetTableProperty(MERGE_ENABLED), MERGE_ENABLED_DEFAULT);
	config.min_count_to_merge =
	    ParseIntProperty<idx_t>(metadata.GetTableProperty(MIN_COUNT_TO_MERGE), MIN_COUNT_TO_MERGE_DEFAULT);
	config.target_size_bytes =
	    ParseIntProperty<int64_t>(metadata.GetTableProperty(TARGET_SIZE_BYTES), TARGET_SIZE_BYTES_DEFAULT);
	return config;
}

//===--------------------------------------------------------------------===//
// Bin-packing (pure logic)
//===--------------------------------------------------------------------===//
vector<vector<idx_t>> BinPackManifests(const vector<int64_t> &weights, int64_t target_weight) {
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
bool ShouldMergeBin(const vector<MergeInputManifest> &input, const vector<idx_t> &bin, idx_t min_count_to_merge) {
	if (bin.size() <= 1) {
		return false;
	}
	bool has_new = false;
	for (auto idx : bin) {
		if (input[idx].source == ManifestSource::NEW_THIS_TRANSACTION) {
			has_new = true;
			break;
		}
	}
	//! A bin containing a freshly-created manifest is only merged once it reaches the threshold,
	//! so small new manifests are not repeatedly rewritten together with large old ones.
	if (has_new && bin.size() < min_count_to_merge) {
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
IcebergManifestListEntry ScanManifestEntries(const IcebergManifestListEntry &list_entry,
                                             IcebergCommitState &commit_state, int32_t schema_id) {
	vector<IcebergManifestListEntry> manifest_files;
	manifest_files.push_back(list_entry);

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

namespace {

//! Merge one spec-homogeneous bin into a single new manifest. Returns the new list entry.
IcebergManifestListEntry MergeBin(const vector<MergeInputManifest> &input, const vector<idx_t> &bin,
                                  IcebergManifestContentType content, CopyFunction &avro_copy, DatabaseInstance &db,
                                  IcebergCommitState &commit_state, int32_t schema_id, int32_t partition_spec_id,
                                  int64_t snapshot_id) {
	auto &table_metadata = commit_state.table_info.table_metadata;
	const bool is_v3 = table_metadata.iceberg_version >= 3;

	//! Gather every entry from the bin's manifests, preserving status and historical sequence
	//! numbers. New (ADDED) entries keep inherited sequence numbers; everything else keeps its
	//! materialized historical value (design doc A1).
	vector<IcebergManifestEntry> merged_entries;
	//! Merging is a pure physical repack: it creates no new rows, so V3 row lineage must be
	//! preserved, not reassigned. The first_row_id is a manifest-file-level value, so the merged
	//! manifest's first_row_id is the smallest first_row_id among the manifests it absorbs (design
	//! doc B2). This uses file-level metadata only -- no entry read required.
	bool has_first_row_id = false;
	int64_t min_first_row_id = 0;
	//! The merged manifest's min_sequence_number must be the smallest data_sequence_number among its
	//! entries. CreateFromEntries cannot compute this (it derives it from the manifest-file sequence
	//! number, which is a placeholder here), so we track the true minimum and set it ourselves before
	//! the manifest is written / handed to AddNewManifestFile. If left as the placeholder, scan
	//! planning's `seq > X` pruning would mis-judge which historical data the manifest can contain
	//! (design doc Phase B item 4).
	bool has_min_seq = false;
	int64_t min_seq = 0;
	for (auto idx : bin) {
		auto &member = input[idx].entry;
		const bool carried_over = input[idx].source == ManifestSource::CARRIED_OVER;
		if (is_v3 && member.file.has_first_row_id) {
			if (!has_first_row_id || member.file.first_row_id < min_first_row_id) {
				min_first_row_id = member.file.first_row_id;
				has_first_row_id = true;
			}
		}
		auto loaded = member.manifest_entries.empty() ? ScanManifestEntries(member, commit_state, schema_id) : member;
		for (auto &entry : loaded.manifest_entries) {
			//! DELETED entries: only deletes made by THIS snapshot are carried into the new manifest;
			//! deletes from earlier snapshots are dropped (they already took effect and would only bloat
			//! the merged manifest forever). Mirrors Java ManifestMergeManager.createManifest.
			if (entry.status == IcebergManifestEntryStatusType::DELETED) {
				if (entry.HasSnapshotId() && entry.GetSnapshotId() == snapshot_id) {
					merged_entries.push_back(std::move(entry));
				}
				continue;
			}
			//! Entries absorbed from an already-committed manifest are EXISTING in the new snapshot:
			//! demote their ADDED status (the file was added by an earlier snapshot, not this one) so
			//! the merged manifest's added/existing counts and the snapshot summary stay correct
			//! (design doc A1). Their historical sequence numbers are preserved by GetSequenceNumber.
			//! Entries from this transaction's own new manifest keep ADDED (genuinely new this commit).
			if (carried_over && entry.status == IcebergManifestEntryStatusType::ADDED) {
				auto seq = entry.GetSequenceNumber(member.file);
				auto file_seq = entry.GetFileSequenceNumber(member.file);
				entry.SetSequenceNumber(seq);
				entry.SetFileSequenceNumber(file_seq);
				entry.status = IcebergManifestEntryStatusType::EXISTING;
			}
			//! Live entries determine the manifest's minimum data sequence number.
			auto entry_seq = entry.GetSequenceNumber(member.file);
			if (!has_min_seq || entry_seq < min_seq) {
				min_seq = entry_seq;
				has_min_seq = true;
			}
			merged_entries.push_back(std::move(entry));
		}
	}

	//! A bin can collapse to nothing (e.g. every entry was a DELETED-by-an-earlier-snapshot entry and
	//! was filtered out above). An empty manifest must never be written -- WriteToFile asserts on it
	//! (and an empty Avro manifest is meaningless). Return an entry with no manifest_entries so the
	//! caller drops it; do this BEFORE CreateFromEntries/WriteToFile (design doc B12).
	if (merged_entries.empty()) {
		IcebergManifestListEntry empty {IcebergManifestFile {""}};
		return empty;
	}

	//! CreateFromEntries computes all counts, min_sequence_number and the partition field summary
	//! from the entries; pass the bin's own spec id so the summary is computed against the correct
	//! (possibly historical) spec, not the table default (design doc D1). We pass a throwaway row-id
	//! counter so the global one is not advanced, then restore the lineage-preserving value below.
	int64_t scratch_row_id = 0;
	auto result = IcebergManifestListEntry::CreateFromEntries(FileSystem::GetFileSystem(commit_state.context),
	                                                          /*snapshot_id*/ -1, /*sequence_number*/ 0, table_metadata,
	                                                          content, std::move(merged_entries), scratch_row_id,
	                                                          partition_spec_id);
	//! first_row_id applies to DATA manifests only; a DELETE manifest's first_row_id is always null.
	if (is_v3 && content == IcebergManifestContentType::DATA) {
		result.file.has_first_row_id = has_first_row_id;
		result.file.first_row_id = has_first_row_id ? min_first_row_id : 0;
	} else {
		result.file.has_first_row_id = false;
	}
	//! Set the true minimum data sequence number from the absorbed entries (see above). Done before
	//! WriteToFile / AddNewManifestFile so scan-planning pruning sees the correct lower bound.
	if (has_min_seq) {
		result.file.has_min_sequence_number = true;
		result.file.min_sequence_number = min_seq;
	}

	auto manifest_length = manifest_file::WriteToFile(table_metadata, result.file, result.manifest_entries, avro_copy,
	                                                  db, commit_state.context, commit_state.manifest_avro_codec);
	result.file.manifest_length = manifest_length;
	return result;
}

} // namespace

vector<IcebergManifestListEntry> MergeManifests(vector<MergeInputManifest> &&input, IcebergManifestContentType content,
                                                const ManifestMergeConfig &config, CopyFunction &avro_copy,
                                                DatabaseInstance &db, IcebergCommitState &commit_state,
                                                int32_t schema_id, int64_t snapshot_id) {
	vector<IcebergManifestListEntry> result;
	if (!config.enabled || input.size() <= 1) {
		for (auto &member : input) {
			result.push_back(std::move(member.entry));
		}
		return result;
	}

	//! Group by partition spec id: only manifests with the same spec may be merged together.
	map<int32_t, vector<idx_t>> by_spec;
	for (idx_t i = 0; i < input.size(); i++) {
		by_spec[input[i].entry.file.partition_spec_id].push_back(i);
	}

	for (auto &spec_group : by_spec) {
		auto spec_id = spec_group.first;
		auto &group_indices = spec_group.second;

		//! Bin-pack using manifest-file-level lengths only (no entries read yet, design doc "decide
		//! first, read later"). Indices in `bins` are into `group_indices`.
		vector<int64_t> weights;
		weights.reserve(group_indices.size());
		for (auto idx : group_indices) {
			weights.push_back(input[idx].entry.file.manifest_length);
		}
		auto bins = BinPackManifests(weights, config.target_size_bytes);

		for (auto &local_bin : bins) {
			//! Translate local (group) indices back to global input indices.
			vector<idx_t> bin;
			bin.reserve(local_bin.size());
			for (auto local : local_bin) {
				bin.push_back(group_indices[local]);
			}

			if (!ShouldMergeBin(input, bin, config.min_count_to_merge)) {
				for (auto idx : bin) {
					result.push_back(std::move(input[idx].entry));
				}
				continue;
			}

			auto merged = MergeBin(input, bin, content, avro_copy, db, commit_state, schema_id, spec_id, snapshot_id);
			//! A bin can collapse to nothing (e.g. all entries were deleted and filtered out); never
			//! write or reference an empty manifest (design doc B12).
			if (merged.manifest_entries.empty()) {
				continue;
			}
			result.push_back(std::move(merged));
		}
	}
	return result;
}

} // namespace duckdb
