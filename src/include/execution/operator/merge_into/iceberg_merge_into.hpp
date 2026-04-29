#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

struct IcebergMergeInto {
	static void ProjectAndCastForCopy(ClientContext &context, DataChunk &input_chunk, PhysicalOperator &copy_op,
	                                  ExpressionExecutor *expression_executor, DataChunk &projected_chunk,
	                                  DataChunk &cast_chunk);

	static void FinalizeCopyToInsert(Pipeline &pipeline, Event &event, ClientContext &context,
	                                 PhysicalOperator &copy_op, PhysicalOperator &insert_op,
	                                 InterruptState &interrupt_state);
};

} // namespace duckdb
