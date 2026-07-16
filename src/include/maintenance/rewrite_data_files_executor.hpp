#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "maintenance/rewrite_data_files_planner.hpp"

namespace duckdb {

struct IcebergTableInformation;
struct RewriteExecutionResult {
	int64_t rewritten_data_files = 0;
	int64_t added_data_files = 0;
	int64_t rewritten_bytes = 0;
	//! Added files for the REPLACE snapshot.
	vector<IcebergManifestEntry> new_entries;
	//! Input files to remove in the same REPLACE snapshot.
	vector<RewriteCandidate> rewritten_candidates;
};

//! Convert one COPY RETURN_STATS row into an ADDED manifest entry. Partition
//! values and sequence-number semantics come from the frozen rewrite plan.
//! Column stats / bounds are populated from the RETURN_STATS map.
IcebergManifestEntry BuildRewriteManifestEntry(ClientContext &context, const vector<RewriteCandidate> &group,
                                               int64_t starting_sequence_number, int64_t record_count,
                                               const string &produced_file, int64_t file_size_in_bytes,
                                               const Value &column_stats, const IcebergTableMetadata &table_metadata,
                                               const string &table_name);

//! Commit all completed rewrite groups as one Iceberg REPLACE snapshot.
void CommitRewrite(ClientContext &context, const RewritePlan &plan, RewriteExecutionResult &result);

//! Best-effort cleanup for files produced before a rewrite failure.
void CleanupRewriteFiles(ClientContext &context, const IcebergTableInformation &table_info,
                         const vector<string> &produced_paths);

//! Validate that the currently loaded table snapshot still matches the frozen
//! rewrite plan. Empty-table plans require the table to remain snapshot-less.
void ValidateRewriteSnapshot(const RewritePlan &plan, const IcebergTableInformation &table_info, const string &phase);

} // namespace duckdb
