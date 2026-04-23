#include "execution/operator/merge_into/iceberg_merge_insert.hpp"
#include "execution/operator/merge_into/iceberg_merge_into.hpp"

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
	IcebergMergeInto::ProjectAndCastForCopy(context.client, chunk, copy, local_state.expression_executor.get(),
	                                        local_state.chunk, local_state.cast_chunk);
	OperatorSinkInput sink_input {*copy.sink_state, *local_state.copy_sink_state, input.interrupt_state};
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

	IcebergMergeInto::FinalizeCopyToInsert(pipeline, event, context, copy, insert, input.interrupt_state);
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
