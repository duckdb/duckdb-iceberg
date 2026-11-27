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
#include "execution/operator/iceberg_pending_deletes.hpp"

namespace duckdb {

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
//! A copy-on-write UPDATE uses the same operator: the build side also computes each matched row's new values (on V3
//! followed by its _row_id and a NULL _last_updated_sequence_number), and once the probe side has read the matched
//! files, the operator emits those rows in place of the ones it dropped.
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
	//! For an UPDATE: the new value of every table column, then on V3 the row lineage columns, evaluated on the build
	//! side's rows
	vector<unique_ptr<Expression>> update_expressions;
	//! For a MERGE: the build side is the MERGE itself, which reports its row count, and the rows its UPDATE and
	//! DELETE actions removed are collected here while it runs
	shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows;

public:
	static PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                                    IcebergTableSchemaVersion &table);
	//! Rewrites the files a planned MERGE matched, once the MERGE has run
	static PhysicalOperator &PlanMerge(ClientContext &context, PhysicalPlanGenerator &planner,
	                                   IcebergTableSchemaVersion &table, PhysicalOperator &merge_plan,
	                                   shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows);

	//! The rows removed by this DELETE; valid once the build side has finalized
	const IcebergCopyOnWriteDeletes &GetDeletes() const;
	//! The row count the statement returns: the rows deleted or updated, or for a MERGE the rows it merged
	idx_t GetResultCount() const;

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
	                                     vector<unique_ptr<Expression>> update_expressions,
	                                     shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows = nullptr);
	void PushKeepScanFilter(const IcebergCopyOnWriteDeletes &deletes) const;
};

//! A DELETE on a table whose write.delete.mode is copy-on-write rewrites every data file that holds a matching row,
//! instead of writing delete files. It is planned as
//!
//!   WITH raw  AS MATERIALIZED (SELECT filename, file_row_number FROM <the DELETE's scan>),
//!        dict AS MATERIALIZED (SELECT filename, row_number() OVER () AS file_no
//!                              FROM (SELECT DISTINCT filename FROM raw))
//!   SELECT keep.* FROM <table> keep
//!   JOIN dict ON keep.filename = dict.filename
//!   ANTI JOIN (SELECT dict.file_no, raw.file_row_number FROM raw JOIN dict USING (filename)) deletee
//!     ON dict.file_no = deletee.file_no AND keep.file_row_number = deletee.file_row_number
//!
//! where on V3 `keep.*` ends with the row lineage columns _row_id and _last_updated_sequence_number, and the kept
//! rows are written by an IcebergFileRewrite, which replaces the touched files in one snapshot.
//!
//! The paths are long, and the anti join's hash table holds one entry per deleted row, so it holds an 8-byte file
//! number instead: `dict` numbers the touched files, one row per file. `raw` keeps the path, but the scan produces it
//! as one value per chunk, which the materialized CTE stores compactly. The join with dict, whose build side becomes a
//! runtime filter on the keep scan's `filename`, skips the untouched files.
//!
//! DELETE ... USING can match a row more than once, so `deletee` can hold a position twice. The anti join removes the
//! row either way, so the number of deleted rows is not counted on `deletee` but on the probe side, where each row
//! of the table appears once: the rows of the touched files (the join with dict) minus the kept rows (the rows the
//! writer wrote). The counter of the touched rows also records their files, which the rewrite replaces: a file whose
//! rows are all deleted produces no kept row, but its rows still pass the join with dict.
struct IcebergCopyOnWriteDelete {
	static PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                                    IcebergTableSchemaVersion &table);
};

//! Passes its rows through unchanged and counts them; with a file column, also records the distinct files
class IcebergCopyOnWriteRowCounter : public PhysicalOperator {
public:
	IcebergCopyOnWriteRowCounter(PhysicalPlan &physical_plan, PhysicalOperator &child,
	                             optional_idx file_column = optional_idx());

	//! The column with the filename, if the files are recorded
	optional_idx file_column;

	//! The number of rows that passed; valid once its pipeline has finished
	idx_t GetCount() const;
	//! The files of the rows that passed, if recorded; valid once its pipeline has finished
	const unordered_set<string> &GetFiles() const;

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

} // namespace duckdb
