//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/iceberg_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"

#include "execution/operator/iceberg_insert.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "../../catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"

namespace duckdb {

class IcebergUpdate : public PhysicalOperator {
public:
	IcebergUpdate(PhysicalPlan &physical_plan, IcebergTableEntry &table, vector<PhysicalIndex> columns,
	              PhysicalOperator &child, PhysicalOperator &delete_op, vector<unique_ptr<Expression>> expressions,
	              vector<unique_ptr<Expression>> bound_defaults);

	//! The table to update
	IcebergTableEntry &table;
	//! The order of to-be-inserted columns
	vector<PhysicalIndex> columns;
	//! The delete operator for deleting the old data
	PhysicalOperator &delete_op;
	//! Index of the _row_id column in the input scan chunk (v3 only)
	optional_idx row_id_index;
	vector<unique_ptr<Expression>> expressions;

public:
	static IcebergUpdate &PlanUpdateOperator(ClientContext &context, PhysicalPlanGenerator &planner,
	                                         IcebergTableEntry &table, LogicalUpdate &op, PhysicalOperator &child_plan,
	                                         IcebergCopyInput &copy_input);

public:
	// Operator interface
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk, GlobalOperatorState &gstate,
	                                        OperatorState &state) const override;
	OperatorFinalResultType OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                         OperatorFinalizeInput &input) const override;

	bool ParallelOperator() const override {
		return true;
	}

	bool RequiresFinalExecute() const override {
		return true;
	}

	bool RequiresOperatorFinalize() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
