//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/iceberg_copy_to_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

class IcebergTableSchemaVersion;
struct IcebergCopyOptions;

//! The copy operator that writes Iceberg data files, for every write path (INSERT, UPDATE, MERGE INTO
//! and CREATE TABLE AS).
//!
//! For CREATE TABLE AS the table does not exist while the plan is built, so the output path - and with
//! it the partitioning and file rotation options - is unknown until the catalog has created it.
//! PhysicalCopyToFile resolves all of those while building its sink state (and starts preparing the
//! output directory from the path), so the CreateTable request has to happen first.
class IcebergCopyToFile : public PhysicalCopyToFile {
public:
	IcebergCopyToFile(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
	                  unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality,
	                  unique_ptr<BoundCreateTableInfo> ctas_info);

public:
	//! For CTAS: creates the table (once) and installs the resulting copy options. Then defers to
	//! PhysicalCopyToFile, which is the first thing to read the output path.
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	//! Installs the options that depend on the table's location. Called by the planner, and again from
	//! GetGlobalSinkState for a CTAS, once the table it writes to actually exists.
	void ApplyCopyOptions(IcebergCopyOptions &copy_options);

	//! The table this copy created, for a CTAS. Null until GetGlobalSinkState has run; read by the
	//! IcebergInsert above this operator, which has no table entry of its own.
	optional_ptr<IcebergTableSchemaVersion> GetCreatedTable() const;

	bool IsCTAS() const {
		return ctas_info != nullptr;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	void EnsureTableCreated(ClientContext &context) const;

private:
	//! Only set for CREATE TABLE AS; the other write paths target a table that already exists.
	//! Carries both what to create and, via its `schema`, the namespace to create it in.
	unique_ptr<BoundCreateTableInfo> ctas_info;
	//! Guards the CreateTable request and the entry it produces if the copy is part of a CTAS
	mutable mutex create_lock;
	mutable optional_ptr<IcebergTableSchemaVersion> created_table DUCKDB_GUARDED_BY(create_lock);
};

} // namespace duckdb
