#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Plan Merge Into
//===--------------------------------------------------------------------===//
static unique_ptr<MergeIntoOperator> IcebergPlanMergeIntoAction(IcebergCatalog &catalog, ClientContext &context,
                                                                LogicalMergeInto &op, PhysicalPlanGenerator &planner,
                                                                BoundMergeIntoAction &action,
                                                                PhysicalOperator &child_plan) {
	auto result = make_uniq<MergeIntoOperator>();

	result->action_type = action.action_type;
	result->condition = std::move(action.condition);
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	for (auto &constraint : op.bound_constraints) {
		bound_constraints.push_back(constraint->Copy());
	}
	auto return_types = op.types;

	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE: {
		LogicalUpdate update(op.table);
		for (auto &def : op.bound_defaults) {
			update.bound_defaults.push_back(def->Copy());
		}
		update.bound_constraints = std::move(bound_constraints);
		update.expressions = std::move(action.expressions);
		update.columns = std::move(action.columns);
		update.update_is_del_and_insert = action.update_is_del_and_insert;
		auto &update_plan = catalog.PlanUpdate(context, planner, update, child_plan);
		result->op = update_plan;
		auto &dl_update = result->op->Cast<IcebergUpdate>();
		// The row_id comes before the deletion information
		dl_update.row_id_index = child_plan.types.size() - 4;
		break;
	}
	case MergeActionType::MERGE_DELETE: {
		LogicalDelete delete_op(op.table, 0);
		delete_op.expressions.push_back(nullptr);

		vector<LogicalType> row_id_types {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::BIGINT};
		for (idx_t i = 0; i < 2; i++) {
			auto ref = make_uniq<BoundReferenceExpression>(row_id_types[i], op.row_id_start + i + 1);
			delete_op.expressions.push_back(std::move(ref));
		}
		delete_op.bound_constraints = std::move(bound_constraints);
		result->op = catalog.PlanDelete(context, planner, delete_op, child_plan);
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		LogicalInsert insert_op(op.table, 0);
		insert_op.bound_constraints = std::move(bound_constraints);
		for (auto &def : op.bound_defaults) {
			insert_op.bound_defaults.push_back(def->Copy());
		}
		// transform expressions if required
		if (!action.column_index_map.empty()) {
			vector<unique_ptr<Expression>> new_expressions;
			for (auto &col : op.table.GetColumns().Physical()) {
				auto storage_idx = col.StorageOid();
				auto mapped_index = action.column_index_map[col.Physical()];
				if (mapped_index == DConstants::INVALID_INDEX) {
					// push default value
					new_expressions.push_back(op.bound_defaults[storage_idx]->Copy());
				} else {
					// push reference
					new_expressions.push_back(std::move(action.expressions[mapped_index]));
				}
			}
			action.expressions = std::move(new_expressions);
		}
		result->expressions = std::move(action.expressions);
		auto &table_entry = op.table.Cast<IcebergTableEntry>();
		auto &table_info = table_entry.table_info;
		auto &schema = table_info.table_metadata.GetLatestSchema();

		IcebergCopyInput copy_input(context, table_entry, schema);
		auto copy_options = IcebergInsert::GetCopyOptions(context, copy_input, schema);

		auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, nullptr);
		auto &insert = IcebergInsert::PlanInsert(context, planner, table_entry);
		insert.children.push_back(physical_copy);

		auto &merge_insert =
		    planner.Make<IcebergMergeInsert>(insert.types, insert, physical_copy).Cast<IcebergMergeInsert>();
		merge_insert.extra_projections = std::move(copy_options.projection_list);
		result->op = merge_insert;
		break;
	}
	case MergeActionType::MERGE_ERROR:
		result->expressions = std::move(action.expressions);
		break;
	case MergeActionType::MERGE_DO_NOTHING:
		break;
	default:
		throw InternalException("Unsupported merge action");
	}
	return result;
}

PhysicalOperator &IcebergCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalMergeInto &op, PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw NotImplementedException("RETURNING is not implemented for Iceberg yet");
	}
	map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions;

	auto &table_entry = op.table.Cast<IcebergTableEntry>();
	table_entry.PrepareIcebergScanFromEntry(context);

	// plan the merge into clauses
	idx_t update_delete_count = 0;
	for (auto &entry : op.actions) {
		vector<unique_ptr<MergeIntoOperator>> planned_actions;
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_UPDATE ||
			    action->action_type == MergeActionType::MERGE_DELETE) {
				update_delete_count++;
				if (update_delete_count > 1) {
					throw NotImplementedException(
					    "MERGE INTO with DuckLake only supports a single UPDATE/DELETE action currently");
				}
			}
			planned_actions.push_back(IcebergPlanMergeIntoAction(*this, context, op, planner, *action, plan));
		}
		actions.emplace(entry.first, std::move(planned_actions));
	}

	auto &result = planner.Make<PhysicalMergeInto>(op.types, std::move(actions), op.row_id_start, op.source_marker,
	                                               true, op.return_chunk);
	result.children.push_back(plan);
	return result;
}

} // namespace duckdb
