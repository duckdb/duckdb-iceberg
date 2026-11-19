//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/iceberg_copy_on_write_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class IcebergTableSchemaVersion;
class LogicalDelete;
class PhysicalPlanGenerator;

//! The data files a copy-on-write DELETE rewrites, and how many rows it removes from them.
struct IcebergCopyOnWriteDeletes {
	unordered_set<string> files;
	idx_t deleted_count = 0;
};

//! A DELETE on a table whose write.delete.mode is copy-on-write rewrites every data file that holds a matching row,
//! instead of writing delete files. It is planned as
//!
//!   WITH deletee AS MATERIALIZED (SELECT DISTINCT filename, file_row_number FROM <the DELETE's scan>)
//!   SELECT keep.* FROM <table> keep
//!   SEMI JOIN (SELECT DISTINCT filename FROM deletee) touched ON keep.filename = touched.filename
//!   ANTI JOIN deletee ON keep.filename = deletee.filename AND keep.file_row_number = deletee.file_row_number
//!
//! and the kept rows are written by an IcebergFileRewrite, which replaces the touched files in one snapshot.
struct IcebergCopyOnWriteDelete {
	static PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                                    IcebergTableSchemaVersion &table);
};

//! Sits on top of the `deletee` CTE body and passes its rows through unchanged, recording the files they belong to
//! and how many there are. The rewrite needs both: a file whose rows are all deleted produces no kept row, but it
//! still has to be replaced.
class IcebergCopyOnWriteDeleteCollector : public PhysicalOperator {
public:
	IcebergCopyOnWriteDeleteCollector(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                                  idx_t estimated_cardinality);

	//! The rows removed by this DELETE; valid once the CTE body has been fully produced
	const IcebergCopyOnWriteDeletes &GetDeletes() const;

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
