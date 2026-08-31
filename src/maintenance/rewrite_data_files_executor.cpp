#include "maintenance/rewrite_data_files_executor.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/logging/logger.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "catalog/rest/transaction/iceberg_transaction_metadata.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

void AccountSelectedCandidates(const RewritePlan &plan, RewriteExecutionResult &result) {
	result.rewritten_data_files = static_cast<int64_t>(plan.selected_candidates.size());
	result.rewritten_bytes = 0;
	result.rewritten_candidates.clear();
	result.rewritten_candidates.reserve(plan.selected_candidates.size());
	for (auto &candidate : plan.selected_candidates) {
		result.rewritten_bytes += candidate.file_size_in_bytes;
		result.rewritten_candidates.push_back(candidate);
	}
}

void PinRewriteSequenceNumbers(vector<IcebergManifestEntry> &entries, int64_t starting_sequence_number) {
	for (auto &entry : entries) {
		//! Preserve equality-delete applicability after compaction.
		entry.SetSequenceNumber(starting_sequence_number);
	}
}

void ValidateRewriteSnapshot(const RewritePlan &plan, const IcebergTable &table_info, const string &phase) {
	auto snapshot = table_info.table_metadata.GetLatestSnapshot();
	if (plan.starting_snapshot_id < 0) {
		if (snapshot) {
			throw CatalogException(
			    "iceberg_rewrite_data_files: table snapshot changed after planning an empty rewrite during %s", phase);
		}
		return;
	}
	if (!snapshot || !snapshot->snapshot_id || *snapshot->snapshot_id != plan.starting_snapshot_id) {
		throw CatalogException("iceberg_rewrite_data_files: table snapshot changed between planning (%lld) and %s (%s)",
		                       plan.starting_snapshot_id, phase,
		                       snapshot && snapshot->snapshot_id ? std::to_string(*snapshot->snapshot_id) : "none");
	}
}

void CleanupRewriteFiles(ClientContext &context, const IcebergTable &table_info, const vector<string> &produced_paths) {
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &path : produced_paths) {
		try {
			fs.TryRemoveFile(path);
		} catch (...) {
			DUCKDB_LOG_DEBUG(context, "Failed to clean up rewrite output file '%s'", path);
		}
	}
}

void CommitRewrite(ClientContext &context, const RewritePlan &plan, RewriteExecutionResult &result) {
	if (result.new_entries.empty()) {
		return;
	}
	if (!plan.table_info) {
		throw InternalException("iceberg_rewrite_data_files: rewrite plan has no table information");
	}
	auto &table_info = *plan.table_info;
	ValidateRewriteSnapshot(plan, table_info, "commit");

	auto &iceberg_transaction = IcebergTransaction::Get(context, table_info.catalog);
	IcebergManifestDeletes deletes;
	for (auto &cand : result.rewritten_candidates) {
		deletes.InvalidateFile(cand.file_path);
	}

	ApplyTableUpdate(table_info, iceberg_transaction, [&](IcebergTable &tbl) {
		ValidateRewriteSnapshot(plan, tbl, "transaction commit");
		auto &transaction_data = tbl.GetOrCreateTransactionData(iceberg_transaction);
		transaction_data.AddSnapshot(IcebergSnapshotOperationType::REPLACE, std::move(result.new_entries),
		                             std::move(deletes));
	});
}

} // namespace duckdb
