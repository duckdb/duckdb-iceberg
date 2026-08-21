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

struct IcebergTable;

struct RewriteCandidate {
	string file_path;
	int64_t file_size_in_bytes = 0;
	int64_t record_count = 0;
	vector<IcebergPartitionInfo> partition_info;
};

struct RewritePlan {
	QualifiedName table_name;
	int64_t starting_snapshot_id = -1;
	int64_t starting_sequence_number = 0;
	//! Iceberg spec default for write.target-file-size-bytes (same as IcebergCopyOptions::file_size_bytes).
	int64_t target_file_size_bytes = 512LL * 1024 * 1024;
	//! Keep the loaded metadata alive until commit.
	shared_ptr<IcebergTable> table_info;
	//! All live DATA files considered during planning.
	vector<RewriteCandidate> candidates;
	//! Files selected for rewrite after per-partition size / min_input_files gating.
	vector<RewriteCandidate> selected_candidates;
};

struct RewriteDataFilesPlanInput {
	QualifiedName table_name;
	optional<int64_t> target_file_size_bytes;
	//! Optional override; defaults to 75% of the resolved target file size.
	optional<int64_t> min_file_size_bytes;
	//! Optional override; defaults to 180% of the resolved target file size.
	optional<int64_t> max_file_size_bytes;
	int64_t min_input_files = 5;
	bool rewrite_all = false;
};

RewritePlan PlanRewrite(ClientContext &context, const RewriteDataFilesPlanInput &input);

namespace rewrite_planner_internal {

//! Canonical partition key used by the bin-packer.
string PartitionBucketKey(const vector<IcebergPartitionInfo> &partition_info);

} // namespace rewrite_planner_internal

} // namespace duckdb
