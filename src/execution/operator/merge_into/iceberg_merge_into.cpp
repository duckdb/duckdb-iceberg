#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "execution/operator/iceberg_insert.hpp"
#include "execution/operator/iceberg_delete.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"

namespace duckdb {

PhysicalOperator &IcebergCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalMergeInto &op, PhysicalOperator &plan) {
	// Iceberg writes a deletion file per data file, so it can apply at most one UPDATE/DELETE to a given row
	idx_t update_delete_count = 0;
	for (auto &entry : op.actions) {
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_UPDATE ||
			    action->action_type == MergeActionType::MERGE_DELETE) {
				update_delete_count++;
			}
		}
	}
	if (update_delete_count > 1) {
		throw NotImplementedException("MERGE INTO with Iceberg only supports a single UPDATE/DELETE action currently");
	}
	auto &irc_transaction = IcebergTransaction::Get(context, *this);
	// an insert-only MERGE writes no delete files
	if (update_delete_count > 0) {
		auto &table_entry = op.table.Cast<IcebergTableSchemaVersion>();
		auto &updated_table = irc_transaction.GetOrCreateAlter().GetOrInitializeTable(table_entry.table_info);
		VerifyMergeOnRead(updated_table.table_metadata, table_entry.name.GetIdentifierName(), WRITE_MERGE_MODE);
	}
	irc_transaction.planning_merge_into = true;
	try {
		auto &merge_into = Catalog::PlanMergeInto(context, planner, op, plan).Cast<PhysicalMergeInto>();
		for (auto &action : merge_into.actions) {
			if (action->action_type == MergeActionType::MERGE_UPDATE) {
				D_ASSERT(action->op);
				action->op->Cast<IcebergInsert>().is_merge = true;
			} else if (action->action_type == MergeActionType::MERGE_DELETE) {
				D_ASSERT(action->op);
				action->op->Cast<IcebergDelete>().is_merge = true;
			}
		}
		irc_transaction.planning_merge_into = false;
		return merge_into;
	} catch (...) {
		irc_transaction.planning_merge_into = false;
		throw;
	}
}

} // namespace duckdb
