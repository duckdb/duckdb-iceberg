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

class IcebergMultiFileList;
class IcebergTableSchemaVersion;
class LogicalDelete;
class LogicalUpdate;
class PhysicalPlanGenerator;
class PhysicalTableScan;

//! The data files a copy-on-write UPDATE rewrites, and the positions it removes from each.
struct IcebergCopyOnWriteDeletes {
	unordered_map<string, unordered_set<int64_t>> rows_by_file;
	idx_t deleted_count = 0;
};

//! A copy-on-write UPDATE rewrites every data file that holds a matching row. (A copy-on-write DELETE used this
//! operator too; it is now planned as joins, see IcebergCopyOnWriteDelete.) This operator is shaped like a join:
//!  - the build side (children[1]) is the DELETE's own scan, which yields the file and position of each matching row;
//!  - the probe side (children[0]) scans the table again, emitting its columns (on V3 followed by the row lineage
//!    columns _row_id and _last_updated_sequence_number) and then those two virtual columns, and this operator keeps
//!    only the rows of matched files that the DELETE does not remove.
//! The kept rows are written by the IcebergFileRewrite above it, which replaces the matched files in one snapshot.
//! A copy-on-write UPDATE uses the same operator: the build side also computes each matched row's new values, and
//! once the probe side has read the matched files, the operator emits those rows in place of the ones it dropped.
class IcebergCopyOnWriteDeleteFilter : public PhysicalOperator {
public:
	IcebergCopyOnWriteDeleteFilter(PhysicalPlan &physical_plan, vector<LogicalType> types, PhysicalOperator &keep_plan,
	                               PhysicalOperator &match_plan, vector<idx_t> match_row_id_indexes);

	//! The filename and file_row_number columns of the build side
	vector<idx_t> match_row_id_indexes;
	//! The file list of the build-side scan. It has loaded the delete files of every matched data file, which are
	//! removed together with that data file.
	optional_ptr<IcebergMultiFileList> match_file_list;
	//! The probe-side scan, if it is a plain table scan: it gets a runtime filter on the matched files
	optional_ptr<PhysicalTableScan> keep_scan;
	ProjectionIndex keep_scan_filename_index;
	//! Owns the logical plan the probe side was planned from
	unique_ptr<LogicalOperator> keep_logical_plan;
	//! For an UPDATE: the new value of every table column, evaluated on the build side's rows
	vector<unique_ptr<Expression>> update_expressions;

public:
	static PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                                    IcebergTableSchemaVersion &table);

	//! The rows removed by this DELETE; valid once the build side has finalized
	const IcebergCopyOnWriteDeletes &GetDeletes() const;

public:
	// Operator interface (probe side)
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	//! For an UPDATE, emits the updated rows after the probe side's last chunk
	OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk, GlobalOperatorState &gstate,
	                                        OperatorState &state) const override;
	bool RequiresFinalExecute() const override {
		return !update_expressions.empty();
	}

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
	//! An earlier V3 snapshot in this transaction will assign any lineage that still reads NULL.
	bool require_assigned_lineage = false;
	//! Plans the rewrite of the files `match_plan` matches; `update_expressions` is empty for a DELETE
	static PhysicalOperator &PlanRewrite(ClientContext &context, PhysicalPlanGenerator &planner,
	                                     IcebergTableSchemaVersion &table, PhysicalOperator &match_plan,
	                                     vector<idx_t> match_row_id_indexes,
	                                     vector<unique_ptr<Expression>> update_expressions);
	void PushKeepScanFilter(const IcebergCopyOnWriteDeletes &deletes) const;
};

//! The data files a copy-on-write DELETE rewrites.
struct IcebergCopyOnWriteDeletedFiles {
	unordered_set<string> files;
};

//! A DELETE on a table whose write.delete.mode is copy-on-write rewrites every data file that holds a matching row,
//! instead of writing delete files. It is planned as
//!
//!   WITH deletee AS MATERIALIZED (SELECT filename, file_row_number FROM <the DELETE's scan>)
//!   SELECT keep.* FROM <table> keep
//!   SEMI JOIN (SELECT DISTINCT filename FROM deletee) touched ON keep.filename = touched.filename
//!   ANTI JOIN deletee ON keep.filename = deletee.filename AND keep.file_row_number = deletee.file_row_number
//!
//! where on V3 `keep.*` ends with the row lineage columns _row_id and _last_updated_sequence_number, and the kept
//! rows are written by an IcebergFileRewrite, which replaces the touched files in one snapshot.
//!
//! DELETE ... USING can match a row more than once, so `deletee` can hold a position twice. The anti join removes the
//! row either way, so the number of deleted rows is not counted on `deletee` but on the probe side, where each row
//! of the table appears once: the rows of the touched files (the semi join's output) minus the kept rows.
struct IcebergCopyOnWriteDelete {
	static PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                                    IcebergTableSchemaVersion &table);
};

//! Sits on top of the `deletee` CTE body and passes its rows through unchanged, recording the files they belong to.
//! A file whose rows are all deleted produces no kept row, but it still has to be replaced.
class IcebergCopyOnWriteDeleteCollector : public PhysicalOperator {
public:
	IcebergCopyOnWriteDeleteCollector(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                                  idx_t estimated_cardinality);

	//! The file list of the DELETE's scan. It has loaded the delete files of every matched data file, which are
	//! removed together with that data file. Null if the optimizer removed the scan because no rows can match.
	optional_ptr<IcebergMultiFileList> match_file_list;

	//! The files this DELETE removes rows from; valid once the CTE body has been fully produced
	const IcebergCopyOnWriteDeletedFiles &GetDeletes() const;

public:
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	string GetName() const override;
};

//! Passes its rows through unchanged and counts them
class IcebergCopyOnWriteRowCounter : public PhysicalOperator {
public:
	IcebergCopyOnWriteRowCounter(PhysicalPlan &physical_plan, PhysicalOperator &child);

	//! The number of rows that passed; valid once its pipeline has finished
	idx_t GetCount() const;

public:
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	string GetName() const override;
};

} // namespace duckdb
