#include "execution/operator/merge_into/iceberg_merge_update.hpp"
#include "execution/operator/merge_into/iceberg_merge_into.hpp"

#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/physical_operator_states.hpp"

namespace duckdb {

IcebergMergeUpdate::IcebergMergeUpdate(PhysicalPlan &physical_plan, const vector<LogicalType> &types,
                                       IcebergUpdate &update_op, PhysicalOperator &copy_op, PhysicalOperator &insert_op)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), update_op(update_op),
      copy_op(copy_op), insert_op(insert_op) {
}

SourceResultType IcebergMergeUpdate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

SinkResultType IcebergMergeUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<IcebergMergeUpdateGlobalState>();
	auto &lstate = input.local_state.Cast<IcebergMergeUpdateLocalState>();

	lstate.update_output.Reset();
	update_op.Execute(context, chunk, lstate.update_output, *gstate.update_gstate, *lstate.update_lstate);

	if (lstate.update_output.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	IcebergMergeInto::ProjectAndCastForCopy(context.client, lstate.update_output, copy_op,
	                                        lstate.expression_executor.get(), lstate.projected_chunk,
	                                        lstate.cast_chunk);
	OperatorSinkInput copy_input {*copy_op.sink_state, *lstate.copy_lstate, input.interrupt_state};
	copy_op.Sink(context, lstate.cast_chunk, copy_input);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType IcebergMergeUpdate::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<IcebergMergeUpdateGlobalState>();
	auto &lstate = input.local_state.Cast<IcebergMergeUpdateLocalState>();

	DataChunk dummy;
	update_op.FinalExecute(context, dummy, *gstate.update_gstate, *lstate.update_lstate);

	OperatorSinkCombineInput copy_combine {*copy_op.sink_state, *lstate.copy_lstate, input.interrupt_state};
	copy_op.Combine(context, copy_combine);
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType IcebergMergeUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<IcebergMergeUpdateGlobalState>();

	OperatorFinalizeInput update_finalize {*gstate.update_gstate, input.interrupt_state};
	update_op.OperatorFinalize(pipeline, event, context, update_finalize);

	OperatorSinkFinalizeInput copy_finalize {*copy_op.sink_state, input.interrupt_state};
	copy_op.Finalize(pipeline, event, context, copy_finalize);

	IcebergMergeInto::FinalizeCopyToInsert(pipeline, event, context, copy_op, insert_op, input.interrupt_state);
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> IcebergMergeUpdate::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_uniq<IcebergMergeUpdateGlobalState>();
	result->update_gstate = update_op.GetGlobalOperatorState(context);
	copy_op.sink_state = copy_op.GetGlobalSinkState(context);
	return std::move(result);
}

unique_ptr<LocalSinkState> IcebergMergeUpdate::GetLocalSinkState(ExecutionContext &context) const {
	auto result = make_uniq<IcebergMergeUpdateLocalState>();
	result->update_lstate = update_op.GetOperatorState(context);
	result->copy_lstate = copy_op.GetLocalSinkState(context);
	result->update_output.Initialize(context.client, update_op.types);
	if (!extra_projections.empty()) {
		result->expression_executor = make_uniq<ExpressionExecutor>(context.client, extra_projections);
		vector<LogicalType> projected_types;
		for (auto &expr : result->expression_executor->expressions) {
			projected_types.push_back(expr->return_type);
		}
		result->projected_chunk.Initialize(context.client, projected_types);
	}
	result->cast_chunk.Initialize(context.client, copy_op.Cast<PhysicalCopyToFile>().expected_types);
	return std::move(result);
}

} // namespace duckdb
