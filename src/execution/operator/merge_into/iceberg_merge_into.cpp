#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "execution/operator/iceberg_delete.hpp"
#include "execution/operator/iceberg_insert.hpp"

namespace duckdb {

PhysicalOperator &IcebergCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalMergeInto &op, PhysicalOperator &plan) {
	// Each delete-producing action participates in the shared collector, including actions that select no rows.
	idx_t update_delete_count = 0;
	for (auto &entry : op.actions) {
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_UPDATE ||
			    action->action_type == MergeActionType::MERGE_DELETE) {
				update_delete_count++;
			}
		}
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
		auto &result = Catalog::PlanMergeInto(context, planner, op, plan);
		irc_transaction.planning_merge_into = false;
		idx_t delete_action_index = 0;
		auto register_delete = [&](IcebergDelete &delete_op) {
			delete_op.merge_action = true;
			delete_op.merge_delete_action_index = delete_action_index++;
			delete_op.merge_delete_action_count = update_delete_count;
		};
		// Retain MERGE membership on the sinks: the planning flag is no longer set when they finalize.
		for (auto &action : result.Cast<PhysicalMergeInto>().actions) {
			switch (action->action_type) {
			case MergeActionType::MERGE_INSERT:
				action->op->Cast<IcebergInsert>().merge_action = true;
				break;
			case MergeActionType::MERGE_UPDATE: {
				// The UPDATE's insert sink reports completion after its replacement files have been staged.
				auto &insert = action->op->Cast<IcebergInsert>();
				insert.merge_action = true;
				register_delete(insert.update_delete_op->Cast<IcebergDelete>());
				break;
			}
			case MergeActionType::MERGE_DELETE:
				register_delete(action->op->Cast<IcebergDelete>());
				break;
			default:
				break;
			}
		}
		D_ASSERT(delete_action_index == update_delete_count);
		return result;
	} catch (...) {
		irc_transaction.planning_merge_into = false;
		throw;
	}
}

} // namespace duckdb
