#pragma once

#include "maintenance/rewrite_data_files_planner.hpp"

namespace duckdb {

struct IcebergTable;
struct IcebergTransactionData;

//! Validate that the currently loaded table snapshot still matches the frozen
//! rewrite plan. Empty-table plans require the table to remain snapshot-less.
void ValidateRewriteSnapshot(const RewritePlan &plan, const IcebergTable &table_info, const string &phase);

//! Maintenance plans from committed metadata and cannot follow pending table changes.
//! Check both at bind time and again before staging the replacement snapshot.
void ValidateRewriteTransaction(const IcebergTransactionData &transaction_data);

} // namespace duckdb
