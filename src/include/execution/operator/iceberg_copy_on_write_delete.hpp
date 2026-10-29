//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/iceberg_copy_on_write_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class IcebergTableSchemaVersion;
class LogicalDelete;
class PhysicalPlanGenerator;
class PhysicalTableScan;

//! The data files a copy-on-write DELETE rewrites, and the positions it removes from each.
struct IcebergCopyOnWriteDeletes {
	unordered_map<string, unordered_set<int64_t>> rows_by_file;
	idx_t deleted_count = 0;
};

//! A DELETE on a table whose write.delete.mode is copy-on-write rewrites every data file that holds a matching row,
//! instead of writing delete files. This operator is shaped like a join:
//!  - the build side (children[1]) is the DELETE's own scan, which yields the file and position of each matching row;
//!  - the probe side (children[0]) scans the table again, emitting its columns followed by those two virtual columns,
//!    and this operator keeps only the rows of matched files that the DELETE does not remove.
//! The kept rows are written by the IcebergInsert above it, which replaces the matched files in one snapshot.
class IcebergCopyOnWriteDeleteFilter : public PhysicalOperator {
public:
	IcebergCopyOnWriteDeleteFilter(PhysicalPlan &physical_plan, vector<LogicalType> types, PhysicalOperator &keep_plan,
	                               PhysicalOperator &match_plan, vector<idx_t> match_row_id_indexes);

	//! The filename and file_row_number columns of the build side
	vector<idx_t> match_row_id_indexes;
	//! The probe-side scan, if it is a plain table scan: it gets a runtime filter on the matched files
	optional_ptr<PhysicalTableScan> keep_scan;
	ProjectionIndex keep_scan_filename_index;
	//! Owns the logical plan the probe side was planned from
	unique_ptr<LogicalOperator> keep_logical_plan;

public:
	static PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                                    IcebergTableSchemaVersion &table);

	//! The rows removed by this DELETE; valid once the build side has finalized
	const IcebergCopyOnWriteDeletes &GetDeletes() const;

public:
	// Operator interface (probe side)
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

public:
	// Sink interface (build side)
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	vector<const_reference<PhysicalOperator>> GetSources() const override;

	string GetName() const override;

private:
	void PushKeepScanFilter(const IcebergCopyOnWriteDeletes &deletes) const;
};

} // namespace duckdb
