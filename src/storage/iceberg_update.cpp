#include "storage/iceberg_update.hpp"
#include "storage/iceberg_delete.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_table_entry.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

IcebergUpdate::IcebergUpdate(PhysicalPlan &physical_plan, ICTableEntry &table, vector<PhysicalIndex> columns_p,
                             PhysicalOperator &child, PhysicalOperator &copy_op, PhysicalOperator &delete_op,
                             PhysicalOperator &insert_op)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), table(table),
      columns(std::move(columns_p)), copy_op(copy_op), delete_op(delete_op), insert_op(insert_op) {
	children.push_back(child);
	row_id_index = columns.size();
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class IcebergUpdateGlobalState : public GlobalSinkState {
public:
	IcebergUpdateGlobalState() : total_updated_count(0) {
	}

	atomic<idx_t> total_updated_count;
};

class IcebergUpdateLocalState : public LocalSinkState {
public:
	unique_ptr<LocalSinkState> copy_local_state;
	unique_ptr<LocalSinkState> delete_local_state;
	DataChunk insert_chunk;
	DataChunk delete_chunk;
	idx_t updated_count = 0;
};

unique_ptr<GlobalSinkState> IcebergUpdate::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_uniq<IcebergUpdateGlobalState>();
	copy_op.sink_state = copy_op.GetGlobalSinkState(context);
	delete_op.sink_state = delete_op.GetGlobalSinkState(context);
	return std::move(result);
}

unique_ptr<LocalSinkState> IcebergUpdate::GetLocalSinkState(ExecutionContext &context) const {
	auto result = make_uniq<IcebergUpdateLocalState>();
	result->copy_local_state = copy_op.GetLocalSinkState(context);
	result->delete_local_state = delete_op.GetLocalSinkState(context);

	vector<LogicalType> delete_types;
	// TODO: Verify these deletes tupes
	delete_types.emplace_back(LogicalType::VARCHAR);
	delete_types.emplace_back(LogicalType::UBIGINT);
	delete_types.emplace_back(LogicalType::BIGINT);

	// updates also write the row id to the file
	auto insert_types = table.GetTypes();

	result->insert_chunk.Initialize(context.client, insert_types);
	result->delete_chunk.Initialize(context.client, delete_types);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType IcebergUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<IcebergUpdateLocalState>();

	// push the to-be-inserted data into the copy
	auto &insert_chunk = lstate.insert_chunk;

	insert_chunk.SetCardinality(chunk.size());
	for (idx_t i = 0; i < columns.size(); i++) {
		insert_chunk.data[columns[i].index].Reference(chunk.data[i]);
	}
	// insert_chunk.data[columns.size()].Reference(chunk.data[row_id_index]);

	OperatorSinkInput copy_input {*copy_op.sink_state, *lstate.copy_local_state, input.interrupt_state};
	copy_op.Sink(context, insert_chunk, copy_input);

	// push the rowids into the delete
	auto &delete_chunk = lstate.delete_chunk;
	delete_chunk.SetCardinality(chunk.size());
	idx_t delete_idx_start = chunk.ColumnCount() - 3;
	for (idx_t i = 0; i < 3; i++) {
		delete_chunk.data[i].Reference(chunk.data[delete_idx_start + i]);
	}

	OperatorSinkInput delete_input {*delete_op.sink_state, *lstate.delete_local_state, input.interrupt_state};
	delete_op.Sink(context, delete_chunk, delete_input);

	lstate.updated_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType IcebergUpdate::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergUpdateGlobalState>();
	auto &local_state = input.local_state.Cast<IcebergUpdateLocalState>();
	OperatorSinkCombineInput copy_combine_input {*copy_op.sink_state, *local_state.copy_local_state,
	                                             input.interrupt_state};
	auto result = copy_op.Combine(context, copy_combine_input);
	if (result != SinkCombineResultType::FINISHED) {
		throw InternalException("IcebergUpdate::Combine does not support async child operators");
	}
	OperatorSinkCombineInput del_combine_input {*delete_op.sink_state, *local_state.delete_local_state,
	                                            input.interrupt_state};
	result = delete_op.Combine(context, del_combine_input);
	if (result != SinkCombineResultType::FINISHED) {
		throw InternalException("IcebergUpdate::Combine does not support async child operators");
	}
	global_state.total_updated_count += local_state.updated_count;
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType IcebergUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	OperatorSinkFinalizeInput copy_finalize_input {*copy_op.sink_state, input.interrupt_state};
	auto result = copy_op.Finalize(pipeline, event, context, copy_finalize_input);
	if (result != SinkFinalizeType::READY) {
		throw InternalException("IcebergUpdate::Finalize does not support async child operators");
	}
	OperatorSinkFinalizeInput del_finalize_input {*delete_op.sink_state, input.interrupt_state};
	result = delete_op.Finalize(pipeline, event, context, del_finalize_input);
	if (result != SinkFinalizeType::READY) {
		throw InternalException("IcebergUpdate::Finalize does not support async child operators");
	}

	// scan the copy operator and sink into the insert operator
	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto global_source = copy_op.GetGlobalSourceState(context);
	auto local_source = copy_op.GetLocalSourceState(execution_context, *global_source);

	DataChunk copy_source_chunk;
	copy_source_chunk.Initialize(context, copy_op.types);

	auto global_sink = insert_op.GetGlobalSinkState(context);
	auto local_sink = insert_op.GetLocalSinkState(execution_context);

	OperatorSourceInput source_input {*global_source, *local_source, input.interrupt_state};
	OperatorSinkInput sink_input {*global_sink, *local_sink, input.interrupt_state};
	while (true) {
		auto source_result = copy_op.GetData(execution_context, copy_source_chunk, source_input);
		if (copy_source_chunk.size() == 0) {
			break;
		}
		if (source_result == SourceResultType::BLOCKED) {
			throw InternalException("IcebergUpdate::Finalize does not support async child operators");
		}

		auto sink_result = insert_op.Sink(execution_context, copy_source_chunk, sink_input);
		if (sink_result == SinkResultType::BLOCKED) {
			throw InternalException("IcebergUpdate::Finalize does not support async child operators");
		}
		if (source_result == SourceResultType::FINISHED) {
			break;
		}
	}

	OperatorSinkFinalizeInput insert_finalize_input {*global_sink, input.interrupt_state};
	result = insert_op.Finalize(pipeline, event, context, insert_finalize_input);
	if (result != SinkFinalizeType::READY) {
		throw InternalException("IcebergUpdate::Finalize does not support async child operators");
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType IcebergUpdate::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<IcebergUpdateGlobalState>();
	auto value = Value::BIGINT(NumericCast<int64_t>(global_state.total_updated_count.load()));
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, value);
	return SourceResultType::FINISHED;
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

PhysicalOperator &IRCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                        PhysicalOperator &child_plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for updates of a Iceberg table");
	}
	for (auto &expr : op.expressions) {
		if (expr->type == ExpressionType::VALUE_DEFAULT) {
			throw BinderException("SET DEFAULT is not yet supported for updates of a Iceberg table");
		}
	}

	auto &table = op.table.Cast<ICTableEntry>();
	// FIXME: we should take the inlining limit into account here and write new updates to the inline data tables if
	// possible updates are executed as a delete + insert - generate the two nodes (delete and insert) plan the copy for
	// the insert

	IcebergCopyInput copy_input(context, table);
	auto &copy_op = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, nullptr);
	// plan the delete
	vector<idx_t> row_id_indexes;
	for (idx_t i = 0; i < 2; i++) {
		row_id_indexes.push_back(i);
	}
	auto &delete_op = IcebergDelete::PlanDelete(context, planner, table, child_plan, std::move(row_id_indexes));
	// plan the actual insert
	auto &insert_op = IcebergInsert::PlanInsert(context, planner, table);

	// IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
	// 		  physical_index_vector_t<idx_t> column_index_map);
	return planner.Make<IcebergUpdate>(table, op.columns, child_plan, copy_op, delete_op, insert_op);
}

void ICTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                         LogicalUpdate &update, ClientContext &context) {
	// all updates in Iceberg are deletes + inserts
	update.update_is_del_and_insert = true;

	// push projections for all columns that are not projected yet
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
