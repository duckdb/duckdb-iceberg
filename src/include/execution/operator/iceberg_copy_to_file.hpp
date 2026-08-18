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

class IcebergCopyToFile : public PhysicalCopyToFile {
public:
	IcebergCopyToFile(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
	                  unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality,
	                  unique_ptr<BoundCreateTableInfo> ctas_info);

public:
	//! For CTAS: creates the table (once) and applies the resulting copy options (field ids & location)
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	//! Apply CopyOptions for Iceberg Tables. Called by the planner, and again from
	//! GetGlobalSinkState for a CTAS, once the table it writes to actually exists.
	void ApplyCopyOptions(IcebergCopyOptions &copy_options);

	//! The table this copy created, for a CTAS. Until GetGlobalSinkState has run this is NULL
	//! Read by the IcebergInsert above this operator, which has no table entry of its own.
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
