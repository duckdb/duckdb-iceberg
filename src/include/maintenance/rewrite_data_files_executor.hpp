#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "maintenance/rewrite_data_files_planner.hpp"

namespace duckdb {

struct IcebergTableInformation;
class IcebergTableSchema;

//! Aggregate counts + the data the commit step needs for the REPLACE snapshot.
//! `new_entries` are the ADDED manifest entries (one per successfully rewritten
//! group). `rewritten_candidates` is the flat list of input files that were
//! actually consumed — the commit step flips their manifest entries to DELETED.
struct RewriteExecutionResult {
	int64_t rewritten_data_files = 0;
	int64_t added_data_files = 0;
	int64_t rewritten_bytes = 0;
	vector<IcebergManifestEntry> new_entries;
	vector<RewriteCandidate> rewritten_candidates;
};

//! Run a single file_group end-to-end: build the COPY SQL, execute it on a
//! fresh Connection so we don't re-enter the calling context, and parse the
//! WRITTEN_FILE_STATISTICS chunk into one ADDED IcebergManifestEntry. Throws on
//! any failure — propagated directly to the caller. Partition info on the
//! returned entry is copied straight from
//! `group.front().partition_info`, relying on the planner's invariant that
//! every file in a bucket shares the same partition values.
//! `starting_sequence_number` is pinned on the new ADDED manifest entry so
//! concurrently-written equality deletes keep applying after compaction
//! (Iceberg V2 spec: delete.seq > data.seq rule). Existing position deletes
//! are materialized during the scan read, not via seq pinning.
IcebergManifestEntry ExecuteOneGroup(ClientContext &context, const IcebergTableInformation &table_info,
                                     const MaintenanceTableKey &table_key,
                                     const vector<RewriteCandidate> &group, int64_t starting_sequence_number);

//! Drive the full plan. No-op (empty result) when `plan.table_is_empty` or
//! `plan.file_groups` is empty. Fail-fast: any group failure throws. The
//! returned `new_entries` accumulate ADDED manifest entries for every group;
//! the commit step uses them as the "added" side of the REPLACE.
RewriteExecutionResult ExecuteRewrite(ClientContext &context, const RewritePlan &plan);

namespace rewrite_executor_internal {

//! Render the FIELD_IDS struct literal for the parquet COPY option.
//! Output: "{'col1': 1, 'col2': {'__duckdb_field_id': 2, 'inner': 3}}".
//! Mirrors `iceberg_insert.cpp::WrittenFieldIds` so the rewrite output is
//! schema-compatible with files written by APPEND. Exposed for unit tests.
string BuildFieldIdsLiteral(const IcebergTableSchema &schema);

//! Build the COPY SQL. Reads via the catalog-attached Iceberg table
//! (`<catalog>.<schema>.<table>`) so the iceberg scan layer applies MoR
//! position/equality deletes during read; filters by `filename` virtual column
//! to scope the scan to the rewrite group. Writes a single output parquet
//! into `target_dir` with `{uuidv7}` filename pattern. RETURN_FILES is on so
//! the caller can parse the produced file list into an IcebergManifestEntry.
//! Exposed for unit tests so the SQL shape is locked down.
string BuildRewriteCopySql(const string &catalog, const string &schema, const string &table,
                           const vector<string> &file_paths, const string &target_dir,
                           const string &field_ids_literal);

} // namespace rewrite_executor_internal

} // namespace duckdb
