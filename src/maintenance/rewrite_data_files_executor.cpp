#include "maintenance/rewrite_data_files_executor.hpp"

#include "duckdb/common/exception.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

void ValidateRewriteSnapshot(const RewritePlan &plan, const IcebergTable &table_info, const string &phase) {
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

void ValidateRewriteTransaction(const IcebergTransactionData &transaction_data) {
	bool pending = !transaction_data.alters.empty();
	for (auto &update : transaction_data.updates) {
		//! A format upgrade is the one change maintenance already follows.
		pending |= update->type != IcebergTableUpdateType::UPGRADE_FORMAT_VERSION;
	}
	if (pending) {
		throw NotImplementedException(
		    "iceberg_rewrite_data_files: table '%s' has uncommitted changes in this transaction; commit them first",
		    transaction_data.table_info.name);
	}
}

void ValidateRewrittenRowCount(const RewritePlan &plan, const vector<IcebergManifestEntry> &new_entries) {
	int64_t written = 0;
	for (auto &entry : new_entries) {
		written += entry.data_file.record_count;
	}
	auto selected = plan.selected_record_count;
	auto at_least = plan.max_deleted_rows ? selected - *plan.max_deleted_rows : int64_t(0);
	if (written > selected || written < at_least) {
		throw CatalogException("iceberg_rewrite_data_files: the rewritten files hold %lld rows, but the %llu files "
		                       "selected hold %lld rows before deletes and at least %lld after them, so the rewrite "
		                       "would change the table's data; nothing was committed",
		                       written, plan.selected_candidates.size(), selected, MaxValue<int64_t>(at_least, 0));
	}
}

} // namespace duckdb
