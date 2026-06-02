//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/physical_iceberg_create_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include <mutex>

namespace duckdb {

class IcebergSchemaEntry;
class IcebergTableEntry;
class PhysicalCopyToFile;

//! Shared mutable state populated at execution time by PhysicalIcebergCreateTable.
//! Read by IcebergInsert during Sink/Finalize so it can resolve the catalog
//! TableCatalogEntry that was created lazily.
struct IcebergCTASCreateState {
	mutex lock;
	//! Set to true once the CreateTable REST call has completed successfully.
	bool created = false;
	//! Populated after CreateTable returns.
	optional_ptr<IcebergTableEntry> table_entry;
};

class IcebergCreateTableGlobalState : public GlobalOperatorState {
public:
	IcebergCreateTableGlobalState() = default;
	std::once_flag init_flag;
};

// Pass through operator between Insert source and CopyToFile/Insert that
// is responsible for calling the IRC api to actually create the table.
class PhysicalIcebergCreateTable : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalIcebergCreateTable(PhysicalPlan &physical_plan, IcebergSchemaEntry &schema_entry,
	                           unique_ptr<BoundCreateTableInfo> info, shared_ptr<IcebergCTASCreateState> create_state,
	                           PhysicalCopyToFile &copy_to_file_op, vector<LogicalType> types,
	                           idx_t estimated_cardinality);

public:
	// Operator interface
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	void MakeCreateTableRequest(ClientContext &context, IcebergCreateTableGlobalState &gstate) const;
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

public:
	IcebergSchemaEntry &schema_entry;
	unique_ptr<BoundCreateTableInfo> info;
	shared_ptr<IcebergCTASCreateState> create_state;
	//! non-const reference to copy_to_file so we can rebind copy options and update field ids
	reference<PhysicalCopyToFile> copy_to_file_op;
};

} // namespace duckdb
