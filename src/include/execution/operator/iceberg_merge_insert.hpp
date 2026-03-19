#pragma once

#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/iceberg_schema_entry.hpp"

namespace duckdb {

//! Wrapper around IcebergInsert for compatibility with PhysicalMergeInto
//! IcebergInsert works by pushing the PhysicalCopyToFile as its child,
//! relying on the executor to Sink everything to the copy operator, and then being fed by the copy operator when it
//! turns into a Source.

//! This doesn't work with PhysicalMergeInto because the MergeIntoOperators aren't managed by the Executor.
//! So we have to wrap this process, sinking into the copy and scanning everything from the copy into the IcebergInsert
//! in the Finalize step.
class IcebergMergeInsert : public PhysicalOperator {
public:
	IcebergMergeInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, PhysicalOperator &insert,
	                   PhysicalOperator &copy);

	//! The copy operator that writes to the file
	PhysicalOperator &copy;
	//! The final insert operator
	PhysicalOperator &insert;
	//! Extra Projections
	vector<unique_ptr<Expression>> extra_projections;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class IcebergMergeIntoLocalState : public LocalSinkState {
public:
	unique_ptr<LocalSinkState> copy_sink_state;
	//! Used if we have extra projections
	DataChunk cast_chunk;
	DataChunk chunk;
	unique_ptr<ExpressionExecutor> expression_executor;
};

} // namespace duckdb
