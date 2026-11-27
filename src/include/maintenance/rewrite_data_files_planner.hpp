#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "core/metadata/iceberg_file_identity.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

struct IcebergTable;

struct RewriteCandidate {
	string file_path;
	int64_t file_size_in_bytes = 0;
	int64_t record_count = 0;
	vector<IcebergPartitionInfo> partition_info;
};

struct RewriteBucket {
	vector<RewriteCandidate> retained;
	//! Counted over every eligible file, so max_files_to_rewrite cannot change the gating decision below.
	int64_t eligible_count = 0;
	int64_t eligible_bytes = 0;
	//! Whether an eligible file reached the delete-file or delete-ratio threshold.
	bool too_many_deletes = false;
};

struct RewritePlan {
	QualifiedName table_name;
	int64_t starting_snapshot_id = -1;
	int64_t starting_sequence_number = 0;
	//! Iceberg spec default for write.target-file-size-bytes (same as IcebergCopyOptions::file_size_bytes).
	int64_t target_file_size_bytes = 512LL * 1024 * 1024;
	//! Keep the loaded metadata alive until commit.
	shared_ptr<IcebergTable> table_info;
	//! Files selected for rewrite after per-partition size / min_input_files gating.
	vector<RewriteCandidate> selected_candidates;
	//! Distinct delete files whose referenced data files were all selected; a deletion vector is identified by its
	//! Puffin path and offset. Invalidated on the same REPLACE commit as the rewritten data files.
	vector<IcebergFileIdentity> delete_files_to_remove;
	//! The rows the selected files hold before deletes: the most the rewrite can write
	int64_t selected_record_count = 0;
	//! The most rows the deletes that may apply to the selected files can remove, so the rewrite writes at least
	//! selected_record_count minus this. Capped at selected_record_count to avoid overflow. Unset when an equality
	//! delete may apply: it can remove any number of rows.
	optional<int64_t> max_deleted_rows = 0;
};

struct RewriteDataFilesPlanInput {
	QualifiedName table_name;
	optional<int64_t> target_file_size_bytes;
	//! Optional override; defaults to 75% of the resolved target file size.
	optional<int64_t> min_file_size_bytes;
	//! Optional override; defaults to 180% of the resolved target file size.
	optional<int64_t> max_file_size_bytes;
	int64_t min_input_files = 5;
	//! Optional upper bound on how many eligible files will be rewritten.
	optional<int64_t> max_files_to_rewrite;
	//! A file with at least this many applicable delete files is rewritten whatever its size. The Iceberg Java
	//! default (Integer.MAX_VALUE) leaves this off.
	int64_t delete_file_threshold = NumericLimits<int32_t>::Maximum();
	//! A file whose file-scoped deletes remove at least this share of its rows is rewritten whatever its size.
	double delete_ratio_threshold = 0.3;
	bool rewrite_all = false;
	//! Spec to rewrite into. Defaults to the table's current default spec (default_spec_id).
	optional<int32_t> output_spec_id;
};

RewritePlan PlanRewrite(ClientContext &context, RewriteDataFilesPlanInput input);

} // namespace duckdb
