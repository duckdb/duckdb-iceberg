//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/iceberg_copy_to_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

class IcebergSchemaEntry;
class IcebergTableSchemaVersion;
struct IcebergCopyOptions;

//! Shared mutable state populated at execution time by IcebergCopyToFile.
//! Read by IcebergInsert during Sink/Finalize so it can resolve the catalog
//! TableCatalogEntry that was created lazily.
struct IcebergCTASCreateState {
	mutex lock;
	//! Set to true once the CreateTable REST call has completed successfully.
	bool created = false;
	//! Populated after CreateTable returns.
	optional_ptr<IcebergTableSchemaVersion> table_entry;
};

//! Everything IcebergCopyToFile needs to issue the CreateTable request for a CREATE TABLE AS.
struct IcebergCTASInfo {
	IcebergCTASInfo(IcebergSchemaEntry &schema_entry, unique_ptr<BoundCreateTableInfo> info,
	                shared_ptr<IcebergCTASCreateState> create_state);

	IcebergSchemaEntry &schema_entry;
	unique_ptr<BoundCreateTableInfo> info;
	shared_ptr<IcebergCTASCreateState> create_state;
};

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
	                  unique_ptr<IcebergCTASInfo> ctas_info);

public:
	//! For CTAS: creates the table (once) and installs the resulting copy options. Then defers to
	//! PhysicalCopyToFile, which is the first thing to read the output path.
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	//! Installs the options that depend on the table's location. Called by the planner, and again from
	//! GetGlobalSinkState for a CTAS, once the table it writes to actually exists.
	void ApplyCopyOptions(IcebergCopyOptions &copy_options);

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	void EnsureTableCreated(ClientContext &context) const;

public:
	//! Only set for CREATE TABLE AS; the other write paths target a table that already exists.
	unique_ptr<IcebergCTASInfo> ctas_info;
};

} // namespace duckdb
