#include "execution/operator/iceberg_update.hpp"

#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "execution/operator/iceberg_delete.hpp"
#include "execution/operator/iceberg_insert.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"

namespace duckdb {

IcebergUpdate::IcebergUpdate(PhysicalPlan &physical_plan, IcebergTableEntry &table, vector<PhysicalIndex> columns_p,
                             PhysicalOperator &child, PhysicalOperator &delete_op_p,
                             vector<unique_ptr<Expression>> expressions_p,
                             vector<unique_ptr<Expression>> bound_defaults)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {}, 1), table(table),
      columns(std::move(columns_p)), delete_op(delete_op_p), expressions(std::move(expressions_p)) {
	children.push_back(child);
	auto &table_metadata = table.table_info.table_metadata;
	if (table_metadata.iceberg_version >= 3) {
		//! For v3, _row_id is the virtual column appended right after physical columns by DuckDB
		row_id_index = columns.size();
	}

	if (!bound_defaults.empty()) {
		//! Replace the DEFAULT expression with the bound version
		D_ASSERT(bound_defaults.size() == expressions.size());
		for (idx_t i = 0; i < expressions.size(); i++) {
			auto &expr = expressions[i];
			if (expr->type == ExpressionType::VALUE_DEFAULT) {
				expr = bound_defaults[i]->Copy();
			}
		}
	}
}

IcebergUpdate &IcebergUpdate::PlanUpdateOperator(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 IcebergTableEntry &table, LogicalUpdate &op,
                                                 PhysicalOperator &child_plan, IcebergCopyInput &copy_input) {
	auto &table_metadata = table.table_info.table_metadata;

	if (table_metadata.HasSortOrder()) {
		auto &sort_spec = table_metadata.GetLatestSortOrder();
		if (sort_spec.IsSorted()) {
			throw NotImplementedException("Update on a sorted iceberg table is not supported yet");
		}
	}
	if (table_metadata.iceberg_version < 2) {
		throw NotImplementedException("Update Iceberg V%d tables", table_metadata.iceberg_version);
	}

	vector<idx_t> row_id_indexes = {0, 1};
	auto &delete_op = IcebergDelete::PlanDelete(context, planner, table, child_plan, std::move(row_id_indexes));

	// build update expressions (physical columns only, no partition cols, no casts)
	vector<unique_ptr<Expression>> expressions;
	unordered_map<idx_t, idx_t> expression_map;
	for (idx_t i = 0; i < op.columns.size(); i++) {
		expression_map[op.columns[i].index] = i;
	}
	for (idx_t i = 0; i < op.columns.size(); i++) {
		expressions.push_back(op.expressions[expression_map[i]]->Copy());
	}

	// Create the IcebergUpdate intermediate operator
	auto &update_op = planner
	                      .Make<IcebergUpdate>(table, op.columns, child_plan, delete_op, std::move(expressions),
	                                           std::move(op.bound_defaults))
	                      .Cast<IcebergUpdate>();

	// Set output types: physical columns + optional _row_id for v3
	vector<LogicalType> update_output_types;
	for (auto &expr : update_op.expressions) {
		update_output_types.push_back(expr->return_type);
	}
	if (table_metadata.iceberg_version >= 3) {
		update_output_types.push_back(LogicalType::BIGINT); // _row_id
	}
	update_op.types = std::move(update_output_types);
	return update_op;
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class IcebergUpdateGlobalState : public GlobalOperatorState {
public:
	IcebergUpdateGlobalState() : total_updated_count(0) {
	}

	atomic<idx_t> total_updated_count;
};

class IcebergUpdateLocalState : public OperatorState {
public:
	unique_ptr<LocalSinkState> delete_local_state;
	unique_ptr<ExpressionExecutor> expression_executor;
	//! Chunk where the updated expressions are executed.
	DataChunk update_expression_chunk;
	DataChunk insert_chunk;
	DataChunk delete_chunk;
	idx_t updated_count = 0;
};

unique_ptr<GlobalOperatorState> IcebergUpdate::GetGlobalOperatorState(ClientContext &context) const {
	auto result = make_uniq<IcebergUpdateGlobalState>();
	delete_op.sink_state = delete_op.GetGlobalSinkState(context);
	return std::move(result);
}

unique_ptr<OperatorState> IcebergUpdate::GetOperatorState(ExecutionContext &context) const {
	auto result = make_uniq<IcebergUpdateLocalState>();
	result->delete_local_state = delete_op.GetLocalSinkState(context);

	vector<LogicalType> expression_types;
	result->expression_executor = make_uniq<ExpressionExecutor>(context.client, expressions);
	for (auto &expr : result->expression_executor->expressions) {
		expression_types.push_back(expr->return_type);
	}

	result->update_expression_chunk.Initialize(context.client, expression_types);
	result->insert_chunk.Initialize(context.client, types);

	vector<LogicalType> delete_types;
	delete_types.emplace_back(LogicalType::VARCHAR);
	delete_types.emplace_back(LogicalType::BIGINT);
	result->delete_chunk.Initialize(context.client, delete_types);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Execute
//===--------------------------------------------------------------------===//
OperatorResultType IcebergUpdate::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                          GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &lstate = state_p.Cast<IcebergUpdateLocalState>();

	// evaluate update expressions
	auto &update_expression_chunk = lstate.update_expression_chunk;
	auto &insert_chunk = lstate.insert_chunk;

	update_expression_chunk.SetCardinality(input.size());
	insert_chunk.SetCardinality(input.size());
	lstate.expression_executor->Execute(input, update_expression_chunk);

	// build output, physical columns + row_id
	const idx_t physical_column_count = columns.size();
	for (idx_t i = 0; i < physical_column_count; i++) {
		insert_chunk.data[i].Reference(update_expression_chunk.data[i]);
	}
	if (row_id_index.IsValid()) {
		// _row_id is the 3rd column from the end in the scan output:
		// [..., _row_id, file_path, seq_row_id]
		auto index = input.ColumnCount() - 3;
		insert_chunk.data[physical_column_count].Reference(input.data[index]);
	}

	chunk.Reference(insert_chunk);

	// Sink the delete tracking columns (last 2 columns: file_path, row_id)
	auto &delete_chunk = lstate.delete_chunk;
	delete_chunk.SetCardinality(input.size());
	idx_t delete_idx_start = input.ColumnCount() - 2;
	for (idx_t i = 0; i < 2; i++) {
		delete_chunk.data[i].Reference(input.data[delete_idx_start + i]);
	}

	InterruptState interrupt_state;
	OperatorSinkInput delete_input {*delete_op.sink_state, *lstate.delete_local_state, interrupt_state};
	delete_op.Sink(context, delete_chunk, delete_input);

	lstate.updated_count += input.size();
	return OperatorResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// FinalExecute
//===--------------------------------------------------------------------===//
OperatorFinalizeResultType IcebergUpdate::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                       GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<IcebergUpdateGlobalState>();
	auto &lstate = state_p.Cast<IcebergUpdateLocalState>();

	InterruptState interrupt_state;
	OperatorSinkCombineInput del_combine_input {*delete_op.sink_state, *lstate.delete_local_state, interrupt_state};
	auto result = delete_op.Combine(context, del_combine_input);
	if (result != SinkCombineResultType::FINISHED) {
		throw InternalException("IcebergUpdate::FinalExecute does not support async child operators");
	}
	gstate.total_updated_count += lstate.updated_count;
	return OperatorFinalizeResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// OperatorFinalize
//===--------------------------------------------------------------------===//
OperatorFinalResultType IcebergUpdate::OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                        OperatorFinalizeInput &input) const {
	auto &iceberg_delete = delete_op.Cast<IcebergDelete>();
	auto &delete_global_state = delete_op.sink_state->Cast<IcebergDeleteGlobalState>();
	auto &iceberg_transaction = IcebergTransaction::Get(context, table.catalog);
	iceberg_delete.FlushDeletes(iceberg_transaction, context, delete_global_state);
	return OperatorFinalResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string IcebergUpdate::GetName() const {
	return "ICEBERG_UPDATE";
}

InsertionOrderPreservingMap<string> IcebergUpdate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name;
	return result;
}

PhysicalOperator &IcebergCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                             PhysicalOperator &child_plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for updates of a Iceberg table");
	}

	auto &table_entry = op.table.Cast<IcebergTableEntry>();
	table_entry.PrepareIcebergScanFromEntry(context);

	auto &irc_transaction = IcebergTransaction::Get(context, *this);
	auto &alter = irc_transaction.GetOrCreateAlter();
	auto &updated_table = alter.GetOrInitializeTable(table_entry.table_info);
	auto &table_metadata = updated_table.table_metadata;
	auto &schema = table_metadata.GetLatestSchema();
	auto &updated_table_entry = *updated_table.schema_versions[schema.schema_id];

	// Plan the copy operator with update_op as child.
	// PlanCopyForInsert will add a partition projection on top if needed.
	IcebergCopyInput copy_input(context, table_metadata, schema);
	if (table_metadata.iceberg_version >= 3) {
		copy_input.virtual_columns = IcebergInsertVirtualColumns::WRITE_ROW_ID;
	}
	auto &update_op =
	    IcebergUpdate::PlanUpdateOperator(context, planner, updated_table_entry, op, child_plan, copy_input);

	optional_ptr<PhysicalOperator> plan = &update_op;
	auto &copy_op = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, plan);

	// Plan the insert sink and wire it up
	auto &insert_op = IcebergInsert::PlanInsert(context, planner, updated_table_entry).Cast<IcebergInsert>();
	insert_op.update_delete_op = update_op.delete_op;
	insert_op.children.push_back(copy_op);
	return insert_op;
}

void IcebergTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                              LogicalUpdate &update, ClientContext &context) {
	// all updates in DuckDB-Iceberg are deletes + inserts
	update.update_is_del_and_insert = true;

	// FIXME: this is almost a copy of LogicalUpdate::BindExtraColumns aside from the duplicate elimination
	// add that to main DuckDB
	auto &column_ids = get.GetColumnIds();
	for (auto &column : columns.Physical()) {
		auto physical_index = column.Physical();
		bool found = false;
		for (auto &col : update.columns) {
			if (col == physical_index) {
				found = true;
				break;
			}
		}
		if (found) {
			// already updated
			continue;
		}
		// check if the column is already projected
		optional_idx column_id_index;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (column_ids[i].GetPrimaryIndex() == physical_index.index) {
				column_id_index = i;
				break;
			}
		}
		if (!column_id_index.IsValid()) {
			// not yet projected - add to projection list
			column_id_index = column_ids.size();
			get.AddColumnId(physical_index.index);
		}
		// column is not projected yet: project it by adding the clause "i=i" to the set of updated columns
		update.expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    column.Type(), ColumnBinding(proj.table_index, proj.expressions.size())));
		proj.expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    column.Type(), ColumnBinding(get.table_index, column_id_index.GetIndex())));
		get.AddColumnId(physical_index.index);
		update.columns.push_back(physical_index);
	}
}

} // namespace duckdb
