#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

struct IcebergTableInformation;

struct RewriteCandidate {
	string file_path;
	int64_t file_size_in_bytes = 0;
	int64_t record_count = 0;
	vector<IcebergPartitionInfo> partition_info;
	//! Position of the original manifest entry, used when marking rewritten files as deleted.
	idx_t manifest_idx = 0;
	idx_t entry_idx = 0;
};

struct RewritePlan {
	QualifiedName table_name;
	int64_t starting_snapshot_id = -1;
	int64_t starting_sequence_number = 0;
	int64_t target_file_size_bytes = 134217728;
	//! Keep the loaded metadata alive until commit.
	shared_ptr<IcebergTableInformation> table_info;
	vector<RewriteCandidate> candidates;
	//! Partition-local rewrite groups.
	vector<vector<RewriteCandidate>> file_groups;
};

struct RewriteDataFilesPlanInput {
	QualifiedName table_name;
	optional<int64_t> target_file_size_bytes;
	int64_t min_input_files = 5;
	bool rewrite_all = false;
};

RewritePlan PlanRewrite(ClientContext &context, const RewriteDataFilesPlanInput &input);

namespace rewrite_planner_internal {

//! Canonical partition key used by the bin-packer.
string PartitionBucketKey(const vector<IcebergPartitionInfo> &partition_info);

} // namespace rewrite_planner_internal

} // namespace duckdb
