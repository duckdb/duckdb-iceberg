//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/copy_on_write/iceberg_copy_on_write_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "execution/operator/iceberg_file_rewrite.hpp"
#include "execution/operator/iceberg_pending_deletes.hpp"

namespace duckdb {

class IcebergMultiFileList;
struct IcebergTable;

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

//! Passes the kept rows of a V3 copy-on-write rewrite through, refusing any whose lineage is still NULL. Such a row
//! was inserted earlier in this transaction, and a NULL written here would inherit from the replacement file,
//! changing the identity or last update of an unchanged row between the two snapshots published at commit.
class IcebergCopyOnWriteLineageCheck : public PhysicalOperator {
public:
	IcebergCopyOnWriteLineageCheck(PhysicalPlan &physical_plan, PhysicalOperator &child, string statement);

	//! DELETE, UPDATE or MERGE, for the error
	string statement;

public:
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	string GetName() const override;
};

//! What a copy-on-write MERGE reports to its rewrite
struct IcebergCopyOnWriteMergeResult {
	//! The row count the MERGE itself returns
	atomic<idx_t> merged_count {0};
	//! The positions its UPDATE and DELETE actions removed, counted with duplicates
	atomic<idx_t> removed_positions {0};
};

//! The body of `raw` for a MERGE. Its child is the planned MERGE: as a sink it takes the MERGE's row count, and once
//! the MERGE has run (its UPDATE and DELETE actions have handed the positions they removed to merge_rows), it is the
//! source of those positions as (filename, file_row_number), one file per chunk. Each file's positions are freed once
//! emitted.
class IcebergCopyOnWriteMergePositions : public PhysicalOperator {
public:
	IcebergCopyOnWriteMergePositions(PhysicalPlan &physical_plan, PhysicalOperator &merge_plan,
	                                 shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows,
	                                 shared_ptr<IcebergCopyOnWriteMergeResult> result);

	shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows;
	shared_ptr<IcebergCopyOnWriteMergeResult> result;

public:
	// Sink: the MERGE emits only its row count
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

	// Source: the removed positions
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	string GetName() const override;
};

//! Writes the rows of a copy-on-write DELETE, UPDATE or MERGE and replaces the touched files in one snapshot. Returns
//! the statement's row count: the rows a DELETE removed, the rows an UPDATE updated, or what a MERGE reported.
class IcebergCopyOnWriteRewrite : public IcebergFileRewrite {
public:
	IcebergCopyOnWriteRewrite(PhysicalPlan &physical_plan, IcebergTable &table,
	                          IcebergCopyOnWriteRowCounter &touched_rows,
	                          optional_ptr<IcebergCopyOnWriteRowCounter> updated_rows,
	                          shared_ptr<IcebergCopyOnWriteMergeResult> merge_result,
	                          const IcebergMultiFileList &target_files, unique_ptr<LogicalOperator> logical_plan);

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

protected:
	IcebergFileRewriteInput GetRewriteInput(const IcebergFileRewriteGlobalState &state) const override;
	void StageRewrite(IcebergTransactionData &transaction_data, const IcebergFileRewriteGlobalState &state,
	                  IcebergSnapshotOperationType operation, vector<IcebergManifestEntry> &&new_entries,
	                  IcebergManifestDeletes &&replaced_files) const override;

private:
	//! Every live row of a touched file is either deleted or kept. `deletee` may list a row twice, the probe side
	//! cannot, so this is the number of distinct deleted rows. The kept rows are the rows the writer wrote.
	idx_t GetDeletedCount(idx_t kept) const;

	IcebergTable &table;
	//! The rows of the touched files (the join with dict's output), which also records the files
	IcebergCopyOnWriteRowCounter &touched_rows;
	//! For an UPDATE: the updated rows, written after the kept ones
	optional_ptr<IcebergCopyOnWriteRowCounter> updated_rows;
	//! For a MERGE: its row count and the number of positions it removed
	shared_ptr<IcebergCopyOnWriteMergeResult> merge_result;
	//! The file list of the keep scan, which reads every touched file. It has loaded their delete files, which are
	//! removed together with them. Not the DELETE's own scan: in a DELETE ... USING, the scan that a matched row's
	//! filename comes from can be the source's.
	const IcebergMultiFileList &target_files;
	//! Owns the logical plan the rewritten rows were planned from
	unique_ptr<LogicalOperator> logical_plan;
};

} // namespace duckdb
