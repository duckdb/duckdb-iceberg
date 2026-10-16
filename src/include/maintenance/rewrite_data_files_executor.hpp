#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "maintenance/rewrite_data_files_planner.hpp"

namespace duckdb {

struct IcebergTable;
struct RewriteExecutionResult {
	int64_t rewritten_data_files = 0;
	int64_t added_data_files = 0;
	int64_t rewritten_bytes = 0;
	//! Added files for the REPLACE snapshot.
	vector<IcebergManifestEntry> new_entries;
	//! Input files to remove in the same REPLACE snapshot.
	vector<RewriteCandidate> rewritten_candidates;
};

//! Commit all completed rewrite output as one Iceberg REPLACE snapshot.
void CommitRewrite(ClientContext &context, const RewritePlan &plan, RewriteExecutionResult &result);

//! Best-effort cleanup for files produced before a rewrite failure.
void CleanupRewriteFiles(ClientContext &context, const IcebergTable &table_info, const vector<string> &produced_paths);

//! Validate that the currently loaded table snapshot still matches the frozen
//! rewrite plan. Empty-table plans require the table to remain snapshot-less.
void ValidateRewriteSnapshot(const RewritePlan &plan, const IcebergTable &table_info, const string &phase);

//! Account selected input files once into the rewrite execution result.
void AccountSelectedCandidates(const RewritePlan &plan, RewriteExecutionResult &result);

//! Pin rewrite sequence numbers on entries produced via IcebergInsert::AddFiles.
void PinRewriteSequenceNumbers(vector<IcebergManifestEntry> &entries, int64_t starting_sequence_number);

} // namespace duckdb
