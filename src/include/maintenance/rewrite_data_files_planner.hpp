#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "maintenance/table_lock_registry.hpp"

namespace duckdb {

struct IcebergTableInformation;

//! One data file eligible for rewrite. Carries enough information for the
//! bin-pack planner (size + partition values) and for the executor (path +
//! record_count for metrics). Mirrors Spark's `FileScanTask` shape.
struct RewriteCandidate {
	string file_path;
	int64_t file_size_in_bytes = 0;
	int64_t record_count = 0;
	//! Partition values in spec order. Empty for unpartitioned tables. Used by
	//! the bin-pack planner to group files into per-partition buckets.
	vector<IcebergPartitionInfo> partition_info;
	int32_t partition_spec_id = 0;
	//! Provenance: position inside the loaded manifest list. Used by the commit step
	//! to mark the original IcebergManifestEntry as DELETED in the REPLACE
	//! snapshot. Stable for the lifetime of one RewritePlan only.
	idx_t manifest_idx = 0;
	idx_t entry_idx = 0;
};

//! Frozen-time view of the table snapshot we plan to rewrite against.
//! `starting_snapshot_id` is used for defensive assertions; the actual CAS
//! (`assert-ref-snapshot-id`) is derived from the table_info metadata at
//! commit time by IcebergTransaction. `starting_sequence_number` is pinned
//! on new ADDED entries so concurrent equality deletes keep applying.
struct RewritePlan {
	bool table_is_empty = false;
	//! Three-part catalog identifier. Executor uses it to build the scan SQL
	//! that routes through the iceberg scan layer (applies MoR deletes).
	MaintenanceTableKey table_key;
	int64_t starting_snapshot_id = -1;
	int64_t starting_sequence_number = 0;
	//! Keep the loaded table metadata alive while the executor opens a nested
	//! connection to run COPY. The nested query can refresh the catalog entry
	//! backing IcebergTableSet; a borrowed pointer would dangle before commit.
	//! Null only when table_is_empty.
	shared_ptr<IcebergTableInformation> table_info;
	//! All data files the planner saw, before any size/partition filtering.
	//! Kept for diagnostics and for the executor's "input bytes" metric.
	vector<RewriteCandidate> candidates;
	//! Files bucketed into rewrite groups. Each inner vector is the input
	//! for one output parquet file. All files in one group belong to
	//! the same partition; total size ≤ target_file_size_bytes (best effort —
	//! a single oversized file may exceed it). Groups with fewer than
	//! min_input_files entries are dropped here unless rewrite_all=true.
	vector<vector<RewriteCandidate>> file_groups;
};

//! Caller-supplied parameters mirroring the SQL-level binds. Kept narrow so
//! the planner does not see the entire RewriteDataFilesBindData (which also
//! holds executor/committer knobs).
struct RewriteDataFilesPlanInput {
	MaintenanceTableKey table_key;
	int64_t target_file_size_bytes = 134217728;
	int64_t min_input_files = 5;
	bool rewrite_all = false;
};

//! Resolve the catalog → schema → table chain, load the latest snapshot's
//! manifest list, and collect every non-deleted data file into a RewritePlan.
//! Throws InvalidInputException for caller mistakes (catalog/schema/table
//! missing, wrong catalog type), HTTPException/IOException on REST failures,
//! and NotImplementedException for unsupported metadata shapes.
RewritePlan PlanRewrite(ClientContext &context, const RewriteDataFilesPlanInput &input);

namespace rewrite_planner_internal {

//! Canonical partition key used by the bin-packer to bucket files. Stable
//! across runs (sorted by field_id) and NULL-tolerant. Exposed for unit tests.
string PartitionBucketKey(const vector<IcebergPartitionInfo> &partition_info);

//! First-Fit Decreasing bin-pack over RewriteCandidates within a single
//! partition. Sorts by size descending, then for each file places it into the
//! first bin that still has room (≤ target_size), opening a new bin otherwise.
//! Mirrors Spark's `BinPacking.ListPacker`. Exposed for unit tests.
vector<vector<RewriteCandidate>> BinPackPartition(vector<RewriteCandidate> files, int64_t target_size);

} // namespace rewrite_planner_internal

} // namespace duckdb
