#include "execution/operator/iceberg_merge_insert.hpp"

#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/physical_operator_states.hpp"

namespace duckdb {

IcebergMergeInsert::IcebergMergeInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types,
                                       PhysicalOperator &insert, PhysicalOperator &copy)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), copy(copy), insert(insert) {
}

SourceResultType IcebergMergeInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

SinkResultType IcebergMergeInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &local_state = input.local_state.Cast<IcebergMergeIntoLocalState>();
	OperatorSinkInput sink_input {*copy.sink_state, *local_state.copy_sink_state, input.interrupt_state};
	reference<DataChunk> chunk_ref = chunk;
	if (!extra_projections.empty()) {
		// We have extra projections, we need to execute them
		local_state.chunk.Reset();
		local_state.expression_executor->Execute(chunk, local_state.chunk);
		chunk_ref = local_state.chunk;
	}

	// Cast the chunk if needed
	auto &copy_types = copy.Cast<PhysicalCopyToFile>().expected_types;
	for (idx_t i = 0; i < chunk_ref.get().ColumnCount(); i++) {
		if (chunk_ref.get().data[i].GetType() != copy_types[i]) {
			VectorOperations::Cast(context.client, chunk_ref.get().data[i], local_state.cast_chunk.data[i],
			                       chunk_ref.get().size());
		} else {
			local_state.cast_chunk.data[i].Reference(chunk_ref.get().data[i]);
		}
	}
	local_state.cast_chunk.SetCardinality(chunk_ref.get().size());

	return copy.Sink(context, local_state.cast_chunk, sink_input);
}

SinkCombineResultType IcebergMergeInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &local_state = input.local_state.Cast<IcebergMergeIntoLocalState>();
	OperatorSinkCombineInput combine_input {*copy.sink_state, *local_state.copy_sink_state, input.interrupt_state};
	return copy.Combine(context, combine_input);
}

SinkFinalizeType IcebergMergeInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              OperatorSinkFinalizeInput &input) const {
	OperatorSinkFinalizeInput copy_finalize {*copy.sink_state, input.interrupt_state};
	auto finalize_result = copy.Finalize(pipeline, event, context, copy_finalize);
	if (finalize_result == SinkFinalizeType::BLOCKED) {
		return SinkFinalizeType::BLOCKED;
	}

	// now scan the copy
	DataChunk chunk;
	chunk.Initialize(context, copy.types);

	ThreadContext thread(context);
	ExecutionContext exec_context(context, thread, nullptr);

	auto copy_global = copy.GetGlobalSourceState(context);
	auto copy_local = copy.GetLocalSourceState(exec_context, *copy_global);
	OperatorSourceInput source_input {*copy_global, *copy_local, input.interrupt_state};

	auto insert_global = insert.GetGlobalSinkState(context);
	auto insert_local = insert.GetLocalSinkState(exec_context);
	OperatorSinkInput sink_input {*insert_global, *insert_local, input.interrupt_state};
	SourceResultType source_res = SourceResultType::HAVE_MORE_OUTPUT;
	while (source_res == SourceResultType::HAVE_MORE_OUTPUT) {
		chunk.Reset();
		source_res = copy.GetData(exec_context, chunk, source_input);
		if (chunk.size() == 0) {
			continue;
		}
		if (source_res == SourceResultType::BLOCKED) {
			throw InternalException("BLOCKED not supported in IcebergMergeInsert");
		}

		auto sink_result = insert.Sink(exec_context, chunk, sink_input);
		if (sink_result != SinkResultType::NEED_MORE_INPUT) {
			throw InternalException("BLOCKED not supported in IcebergMergeInsert");
		}
	}
	OperatorSinkCombineInput combine_input {*insert_global, *insert_local, input.interrupt_state};
	auto combine_res = insert.Combine(exec_context, combine_input);
	if (combine_res == SinkCombineResultType::BLOCKED) {
		throw InternalException("BLOCKED not supported in IcebergMergeInsert");
	}
	OperatorSinkFinalizeInput finalize_input {*insert_global, input.interrupt_state};
	auto finalize_res = insert.Finalize(pipeline, event, context, finalize_input);
	if (finalize_res == SinkFinalizeType::BLOCKED) {
		throw InternalException("BLOCKED not supported in IcebergMergeInsert");
	}
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> IcebergMergeInsert::GetGlobalSinkState(ClientContext &context) const {
	copy.sink_state = copy.GetGlobalSinkState(context);
	return make_uniq<GlobalSinkState>();
}

unique_ptr<LocalSinkState> IcebergMergeInsert::GetLocalSinkState(ExecutionContext &context) const {
	auto result = make_uniq<IcebergMergeIntoLocalState>();
	result->copy_sink_state = copy.GetLocalSinkState(context);
	if (!extra_projections.empty()) {
		result->expression_executor = make_uniq<ExpressionExecutor>(context.client, extra_projections);
		vector<LogicalType> insert_types;
		for (auto &expr : result->expression_executor->expressions) {
			insert_types.push_back(expr->return_type);
		}
		result->chunk.Initialize(context.client, insert_types);
	}
	result->cast_chunk.Initialize(context.client, copy.Cast<PhysicalCopyToFile>().expected_types);

	return std::move(result);
}

} // namespace duckdb
