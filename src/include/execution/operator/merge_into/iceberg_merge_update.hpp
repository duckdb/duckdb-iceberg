#pragma once

#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

#include "execution/operator/iceberg_update.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"

namespace duckdb {

//! Wrapper around IcebergUpdate for compatibility with PhysicalMergeInto
//! IcebergUpdate works by pushing the PhysicalCopyToFile as its child,
//! relying on the executor to Sink everything to the copy operator, and then being fed by the copy operator when it
//! turns into a Source.

//! This doesn't work with PhysicalMergeInto because the MergeIntoOperators aren't managed by the Executor.
//! So we have to wrap this process, sinking into the copy and scanning everything from the copy into the IcebergInsert
//! in the Finalize step.
class IcebergMergeUpdate : public PhysicalOperator {
public:
	IcebergMergeUpdate(PhysicalPlan &physical_plan, const vector<LogicalType> &types, IcebergUpdate &update_op,
	                   PhysicalOperator &copy_op, PhysicalOperator &insert_op);

	IcebergUpdate &update_op;
	PhysicalOperator &copy_op;
	PhysicalOperator &insert_op;
	//! Extra projections for partition columns
	vector<unique_ptr<Expression>> extra_projections;

public:
	string GetName() const override {
		return "ICEBERG_MERGE_UPDATE";
	}

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

class IcebergMergeUpdateGlobalState : public GlobalSinkState {
public:
	unique_ptr<GlobalOperatorState> update_gstate;
};

class IcebergMergeUpdateLocalState : public LocalSinkState {
public:
	unique_ptr<OperatorState> update_lstate;
	unique_ptr<LocalSinkState> copy_lstate;
	DataChunk update_output;
	//! Projection and cast chunks for partition columns (same pattern as IcebergMergeInsert)
	unique_ptr<ExpressionExecutor> expression_executor;
	DataChunk projected_chunk;
	DataChunk cast_chunk;
};

} // namespace duckdb
