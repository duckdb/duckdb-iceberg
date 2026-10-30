#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "execution/operator/iceberg_copy_on_write_delete.hpp"
#include "execution/operator/iceberg_delete.hpp"
#include "execution/operator/iceberg_insert.hpp"

namespace duckdb {

//! A copy-on-write MERGE plans its actions as a merge-on-read MERGE does, except that its UPDATE and DELETE actions
//! hand the rows they remove to a rewrite that runs after the MERGE, instead of writing delete files. That rewrite
//! replaces the matched data files in the snapshot the MERGE's own data files are staged in.
PhysicalOperator &IcebergCatalog::PlanCopyOnWriteMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                           LogicalMergeInto &op, PhysicalOperator &plan,
                                                           IcebergTableSchemaVersion &table) {
	auto &irc_transaction = IcebergTransaction::Get(context, *this);
	op.table.Cast<IcebergTableSchemaVersion>().PrepareIcebergScanFromEntry(context);
	irc_transaction.planning_merge_into = true;
	optional_ptr<PhysicalOperator> merge;
	try {
		merge = Catalog::PlanMergeInto(context, planner, op, plan);
		irc_transaction.planning_merge_into = false;
	} catch (...) {
		irc_transaction.planning_merge_into = false;
		throw;
	}
	auto merge_rows = make_shared_ptr<IcebergCopyOnWriteMergeRows>();
	for (auto &action : merge->Cast<PhysicalMergeInto>().actions) {
		switch (action->action_type) {
		case MergeActionType::MERGE_INSERT:
			action->op->Cast<IcebergInsert>().merge_action = true;
			break;
		case MergeActionType::MERGE_UPDATE: {
			// The updated rows are written as new files; the rows they replace are dropped by the rewrite
			auto &insert = action->op->Cast<IcebergInsert>();
			insert.merge_action = true;
			insert.update_delete_op->Cast<IcebergDelete>().copy_on_write_merge_rows = merge_rows;
			break;
		}
		case MergeActionType::MERGE_DELETE:
			action->op->Cast<IcebergDelete>().copy_on_write_merge_rows = merge_rows;
			break;
		default:
			break;
		}
	}
	return IcebergCopyOnWriteDeleteFilter::PlanMerge(context, planner, table, *merge, std::move(merge_rows));
}

PhysicalOperator &IcebergCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalMergeInto &op, PhysicalOperator &plan) {
	op.table.Cast<IcebergTableSchemaVersion>().table_info.VerifyWriteEncryptionSupported();
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
		auto &table_metadata = updated_table.table_metadata;
		//! As for DELETE and UPDATE, a V3 table refuses a copy-on-write mode below
		if (table_metadata.iceberg_version == 2 && table_metadata.UsesCopyOnWrite(WRITE_MERGE_MODE) &&
		    !op.return_chunk) {
			auto &updated_table_entry = *updated_table.schema_versions[table_metadata.GetLatestSchema().schema_id];
			return PlanCopyOnWriteMergeInto(context, planner, op, plan, updated_table_entry);
		}
		VerifyMergeOnRead(table_metadata, table_entry.name.GetIdentifierName(), WRITE_MERGE_MODE);
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
