#include "maintenance/rewrite_data_files_planner.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "iceberg_options.hpp"
#include "maintenance/maintenance_table_loader.hpp"

#include <algorithm>
#include <map>

namespace duckdb {

namespace {

//! Split candidates into per-partition rewrite groups using BinPackPartition.
//! `min_input_files` drops groups too small to amortise the rewrite cost,
//! unless `rewrite_all=true` (force a rewrite of every eligible file even if
//! only one file lives in that partition).
void GroupCandidates(RewritePlan &plan, int64_t target_file_size_bytes, int64_t min_input_files, bool rewrite_all) {
	if (plan.candidates.empty()) {
		return;
	}

	//! Bucket by canonical partition key. std::map (ordered) so the resulting
	//! file_groups are emitted in a deterministic order — stable ordering
	//! makes tests and diagnostics deterministic.
	std::map<string, vector<RewriteCandidate>> per_partition;
	for (auto &cand : plan.candidates) {
		//! Skip files that are already at or above target unless rewrite_all
		//! demands we rewrite everything (e.g. forced spec migration).
		if (!rewrite_all && cand.file_size_in_bytes >= target_file_size_bytes) {
			continue;
		}
		per_partition[rewrite_planner_internal::PartitionBucketKey(cand.partition_info)].push_back(cand);
	}

	for (auto &kv : per_partition) {
		auto bins = rewrite_planner_internal::BinPackPartition(std::move(kv.second), target_file_size_bytes);
		for (auto &bin : bins) {
			//! Drop groups that don't meet the cost threshold. The rewrite_all
			//! override above already kept the candidates in the bucket — we
			//! still emit them as their own groups so the executor processes
			//! them (mirrors Spark's behaviour: rewrite_all bypasses
			//! min_input_files, NOT the bin-pack itself).
			if (!rewrite_all && static_cast<int64_t>(bin.size()) < min_input_files) {
				continue;
			}
			plan.file_groups.push_back(std::move(bin));
		}
	}
}

} // namespace

namespace rewrite_planner_internal {

string PartitionBucketKey(const vector<IcebergPartitionInfo> &partition_info) {
	if (partition_info.empty()) {
		return "";
	}
	vector<IcebergPartitionInfo> sorted = partition_info;
	std::sort(sorted.begin(), sorted.end(),
	          [](const IcebergPartitionInfo &a, const IcebergPartitionInfo &b) { return a.field_id < b.field_id; });
	//! Length-prefixed encoding to avoid collisions from values containing
	//! delimiters. Format: "field_id:N:value" where N is the byte length of
	//! value (or "NULL" for null). This guarantees different partition tuples
	//! always produce different keys.
	string out;
	for (auto &p : sorted) {
		out += std::to_string(p.field_id);
		out += ":";
		if (p.value.IsNull()) {
			out += "NULL";
		} else {
			auto val_str = p.value.ToString();
			out += std::to_string(val_str.size());
			out += ":";
			out += val_str;
		}
		out += ";";
	}
	return out;
}

vector<vector<RewriteCandidate>> BinPackPartition(vector<RewriteCandidate> files, int64_t target_size) {
	std::sort(files.begin(), files.end(), [](const RewriteCandidate &a, const RewriteCandidate &b) {
		return a.file_size_in_bytes > b.file_size_in_bytes;
	});

	vector<vector<RewriteCandidate>> bins;
	vector<int64_t> bin_sizes;

	for (auto &file : files) {
		int64_t placed_bin = -1;
		for (idx_t i = 0; i < bins.size(); ++i) {
			//! First-fit: scan bins in creation order. Files larger than
			//! target_size never fit anywhere — they get their own bin via
			//! the placed_bin < 0 path below, preserving FFD's invariant that
			//! "one oversized file per bin" is the upper bound on bin count.
			if (bin_sizes[i] + file.file_size_in_bytes <= target_size) {
				placed_bin = static_cast<int64_t>(i);
				break;
			}
		}
		if (placed_bin < 0) {
			bins.emplace_back();
			bin_sizes.push_back(0);
			placed_bin = static_cast<int64_t>(bins.size()) - 1;
		}
		bin_sizes[placed_bin] += file.file_size_in_bytes;
		bins[placed_bin].push_back(std::move(file));
	}
	return bins;
}

} // namespace rewrite_planner_internal

RewritePlan PlanRewrite(ClientContext &context, const RewriteDataFilesPlanInput &input) {
	RewritePlan plan;
	plan.table_key = input.table_key;

	auto table_info_ptr = LoadIcebergTableShared(context, input.table_key, "iceberg_rewrite_data_files");
	auto &table_info = *table_info_ptr;
	auto &table_metadata = table_info.table_metadata;
	plan.table_info = std::move(table_info_ptr);

	if (table_metadata.iceberg_version >= 3) {
		throw NotImplementedException("iceberg_rewrite_data_files does not yet support V3 tables");
	}

	auto latest_snapshot = table_metadata.GetLatestSnapshot();
	if (!latest_snapshot) {
		//! Brand-new table — no snapshot to rewrite against. Caller treats this
		//! as a no-op success; executor and commit step must not run on this plan.
		plan.table_is_empty = true;
		return plan;
	}

	plan.starting_snapshot_id = latest_snapshot->snapshot_id;
	//! Pin the STARTING SNAPSHOT's own sequence_number — NOT the table-global
	//! last_sequence_number. The rewritten files inherit this value at commit
	//! (executor SetSequenceNumber), so it must be the seq of the snapshot we
	//! are rewriting against. The two coincide for a simple main-only history,
	//! but last_sequence_number is the table-wide max: with branches/tags it can
	//! exceed the current snapshot's seq, which would pin the new files ABOVE
	//! the starting seq and let concurrent equality deletes silently stop
	//! applying. Parity: Iceberg RewriteDataFilesCommitManager pins
	//! table.snapshot(startingSnapshotId).sequenceNumber(); Trino finishOptimize
	//! pins snapshot.sequenceNumber() of the optimize snapshot.
	plan.starting_sequence_number = latest_snapshot->sequence_number;

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = latest_snapshot;
	snapshot_info.schema_id = table_metadata.GetCurrentSchemaId();

	//! Loads the manifest list AND scans each manifest file's entries in one
	//! call — populates manifest_entries inside every IcebergManifestListEntry.
	IcebergOptions options;
	auto manifest_list = IcebergManifestList::Load(table_metadata.GetLocation(), table_metadata, snapshot_info,
	                                               context, options);

	const auto &manifest_files = manifest_list->GetManifestFilesConst();
	if (manifest_files.empty()) {
		plan.table_is_empty = true;
		return plan;
	}

	//! MoR delete tables (V2) are handled: the executor reads through the
	//! iceberg scan layer which applies position/equality deletes. V3 tables
	//! are rejected before manifest scan (above).

	auto default_spec_id = table_metadata.default_spec_id;

	for (idx_t mi = 0; mi < manifest_files.size(); ++mi) {
		const auto &list_entry = manifest_files[mi];
		//! Only DATA manifests contain rewriteable files; DELETE manifests are
		//! left untouched here.
		if (list_entry.file.content != IcebergManifestContentType::DATA) {
			continue;
		}
		//! Guard against partition spec evolution: if a manifest was written
		//! under a different partition spec, its partition tuples may not match
		//! the current default spec. Mixing specs in one rewrite group would
		//! produce incorrect manifest metadata. Reject until multi-spec support
		//! is implemented.
		if (list_entry.file.partition_spec_id != default_spec_id) {
			throw NotImplementedException(
			    "iceberg_rewrite_data_files: table has data files written under partition spec %d "
			    "but current default spec is %d; partition spec evolution is not yet supported",
			    list_entry.file.partition_spec_id, default_spec_id);
		}

		const auto &entries = list_entry.manifest_entries;
		for (idx_t ei = 0; ei < entries.size(); ++ei) {
			const auto &entry = entries[ei];
			//! Skip tombstones: they are bookkeeping, not real files on disk.
			if (entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}
			//! Defence in depth — DATA manifests shouldn't carry delete-content
			//! data files, but we filter anyway so partial-format catalogs
			//! don't slip equality deletes into our candidate pool.
			if (entry.data_file.content != IcebergManifestEntryContentType::DATA) {
				continue;
			}

			RewriteCandidate cand;
			cand.file_path = entry.data_file.file_path;
			cand.file_size_in_bytes = entry.data_file.file_size_in_bytes;
			cand.record_count = entry.data_file.record_count;
			cand.partition_info = entry.data_file.partition_info;
			cand.partition_spec_id = list_entry.file.partition_spec_id;
			cand.manifest_idx = mi;
			cand.entry_idx = ei;
			plan.candidates.push_back(std::move(cand));
		}
	}

	if (plan.candidates.empty()) {
		plan.table_is_empty = true;
		return plan;
	}

	GroupCandidates(plan, input.target_file_size_bytes, input.min_input_files, input.rewrite_all);

	//! `candidates` non-empty but every group dropped (all files at/above
	//! target, or no partition cleared min_input_files). Treat as no-op so
	//! the executor doesn't waste cycles on an empty group list.
	if (plan.file_groups.empty()) {
		plan.table_is_empty = true;
	}
	return plan;
}

} // namespace duckdb
