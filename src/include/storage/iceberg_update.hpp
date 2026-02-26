//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/iceberg_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/iceberg_insert.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/iceberg_metadata_info.hpp"
#include "storage/catalog/iceberg_table_entry.hpp"
#include "storage/catalog/iceberg_schema_entry.hpp"

namespace duckdb {

class IcebergUpdate : public PhysicalOperator {
public:
	IcebergUpdate(PhysicalPlan &physical_plan, IcebergTableEntry &table, vector<PhysicalIndex> columns,
	              PhysicalOperator &child, PhysicalOperator &copy_op, PhysicalOperator &delete_op,
	              PhysicalOperator &insert_op);

	//! The table to update
	IcebergTableEntry &table;
	//! The order of to-be-inserted columns
	vector<PhysicalIndex> columns;
	//! The copy operator for writing new data to files
	PhysicalOperator &copy_op;
	//! The delete operator for deleting the old data
	PhysicalOperator &delete_op;
	//! The (final) insert operator that registers inserted data
	PhysicalOperator &insert_op;
	//! The row-id-index
	optional_idx row_id_index;

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

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
