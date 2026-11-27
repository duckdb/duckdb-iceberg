//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/copy_on_write/iceberg_copy_on_write.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "execution/operator/iceberg_pending_deletes.hpp"

namespace duckdb {

class IcebergTableSchemaVersion;
class LogicalDelete;
class LogicalMergeInto;
class LogicalUpdate;
class PhysicalPlanGenerator;
class PhysicalTableScan;

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
//!
//! A copy-on-write UPDATE is planned the same way, with the new version of each matched row in `raw`, and writes the
//! kept rows UNION ALL those new versions. Every live row of a touched file is then written exactly once, unless an
//! UPDATE ... FROM matched a row more than once, which the rewrite detects as more rows written than touched.
struct IcebergCopyOnWriteDelete {
	static PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                                    IcebergTableSchemaVersion &table);
	static PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                                    IcebergTableSchemaVersion &table);
	//! A copy-on-write MERGE: raw is the positions its UPDATE and DELETE actions removed (they hand them to
	//! merge_rows), and it writes only the kept rows; the MERGE writes its new rows itself.
	static PhysicalOperator &PlanMerge(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                   PhysicalOperator &merge_plan, IcebergTableSchemaVersion &table,
	                                   shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows);
};

} // namespace duckdb
