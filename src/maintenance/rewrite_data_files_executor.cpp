#include "maintenance/rewrite_data_files_executor.hpp"

#include "duckdb/common/exception.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "catalog/rest/transaction/iceberg_transaction_metadata.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"

namespace duckdb {

IcebergManifestEntry BuildRewriteManifestEntry(const vector<RewriteCandidate> &group, int64_t starting_sequence_number,
                                               int64_t record_count, const string &produced_file,
                                               int64_t file_size_in_bytes) {
	if (group.empty()) {
		throw InternalException("iceberg_rewrite_data_files: cannot build a manifest entry for an empty group");
	}

	IcebergManifestEntry entry;
	entry.status = IcebergManifestEntryStatusType::ADDED;
	//! Preserve equality-delete applicability after compaction.
	entry.SetSequenceNumber(starting_sequence_number);
	entry.data_file.content = IcebergManifestEntryContentType::DATA;
	entry.data_file.file_format = "parquet";
	entry.data_file.file_path = produced_file;
	entry.data_file.record_count = record_count;
	entry.data_file.file_size_in_bytes = file_size_in_bytes;
	//! The planner buckets candidates so every file in one group shares the same
	//! partition tuple. Reuse candidate 0 instead of re-deriving it.
	entry.data_file.partition_info = group.front().partition_info;
	return entry;
}

void ValidateRewriteSnapshot(const RewritePlan &plan, const IcebergTableInformation &table_info, const string &phase) {
	auto snapshot = table_info.table_metadata.GetLatestSnapshot();
	if (plan.starting_snapshot_id < 0) {
		if (snapshot) {
			throw CatalogException(
			    "iceberg_rewrite_data_files: table snapshot changed after planning an empty rewrite during %s", phase);
		}
		return;
	}
	if (!snapshot || snapshot->snapshot_id != plan.starting_snapshot_id) {
		throw CatalogException("iceberg_rewrite_data_files: table snapshot changed between planning (%lld) and %s (%s)",
		                       plan.starting_snapshot_id, phase,
		                       snapshot ? std::to_string(snapshot->snapshot_id) : "none");
	}
}

void CleanupRewriteFiles(ClientContext &context, const IcebergTableInformation &table_info,
                         const vector<string> &produced_paths) {
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

	ApplyTableUpdate(table_info, iceberg_transaction, [&](IcebergTableInformation &tbl) {
		ValidateRewriteSnapshot(plan, tbl, "transaction commit");
		auto &transaction_data = tbl.GetOrCreateTransactionData(iceberg_transaction);
		transaction_data.AddSnapshot(IcebergSnapshotOperationType::REPLACE, std::move(result.new_entries),
		                             std::move(deletes));
	});
}

} // namespace duckdb
