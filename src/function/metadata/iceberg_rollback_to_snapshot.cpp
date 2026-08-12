#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "function/iceberg_functions.hpp"

namespace duckdb {

namespace {

struct IcebergRollbackToSnapshotBindData : public TableFunctionData {
	optional_ptr<IcebergTableSchemaVersion> iceberg_table;
	int64_t snapshot_id = 0;
};

struct IcebergRollbackToSnapshotGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
		return make_uniq<IcebergRollbackToSnapshotGlobalState>();
	}

	bool finished = false;
};

static bool CheckTableIsIcebergTable(optional_ptr<CatalogEntry> entry) {
	auto &catalog_entry = entry->Cast<InCatalogEntry>();
	auto &catalog = catalog_entry.catalog;
	if (catalog.GetCatalogType() != "iceberg") {
		return false;
	}
	if (entry->type != CatalogType::TABLE_ENTRY) {
		return false;
	}
	return true;
}

static void VerifyInputIsNotAFile(ClientContext &context, string &input_string) {
	auto qualified_name = QualifiedName::ParseComponents(input_string);
	if (qualified_name.size() == 1) {
		auto &fs = FileSystem::GetFileSystem(context);
		if (fs.DirectoryExists(input_string) || fs.FileExists(input_string)) {
			throw InvalidInputException("Cannot call iceberg_rollback_to_snapshot() on a file/directory");
		}
	}
}

static unique_ptr<FunctionData> IcebergRollbackToSnapshotBind(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<Identifier> &names) {
	auto ret = make_uniq<IcebergRollbackToSnapshotBindData>();

	auto input_string = input.inputs[0].ToString();
	VerifyInputIsNotAFile(context, input_string);
	auto parts = QualifiedName::ParseComponents(input_string);
	if (parts.size() != 3) {
		throw InvalidInputException(
		    "iceberg_rollback_to_snapshot: table identifier must be 'catalog.schema.table', got '%s'", input_string);
	}
	for (auto &part : parts) {
		if (part.empty()) {
			throw InvalidInputException("iceberg_rollback_to_snapshot: table identifier '%s' has an empty component",
			                            input_string);
		}
	}

	auto iceberg_table = IcebergUtils::GetTableEntry(context, input_string);
	if (!CheckTableIsIcebergTable(iceberg_table)) {
		throw InvalidInputException("Cannot call iceberg_rollback_to_snapshot on non-iceberg table");
	}
	ret->iceberg_table = iceberg_table->Cast<IcebergTableSchemaVersion>();
	// Accept BIGINT / UBIGINT from iceberg_snapshots without overflowing signed range unexpectedly.
	auto snapshot_value = input.inputs[1].DefaultCastAs(LogicalType::BIGINT);
	ret->snapshot_id = snapshot_value.GetValue<int64_t>();

	if (input.binder) {
		DatabaseModificationType modification;
		modification |= DatabaseModificationType::ALTER_TABLE;
		input.binder->GetStatementProperties().RegisterDBModify(iceberg_table->ParentCatalog(), context, modification);
	}

	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("previous_snapshot_id");
	names.emplace_back("current_snapshot_id");
	return std::move(ret);
}

static void IcebergRollbackToSnapshotFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergRollbackToSnapshotBindData>();
	auto &global_state = data.global_state->Cast<IcebergRollbackToSnapshotGlobalState>();

	if (!bind_data.iceberg_table || global_state.finished) {
		output.SetChildCardinality(0);
		return;
	}

	auto iceberg_table = bind_data.iceberg_table;
	auto &table_info = iceberg_table->table_info;
	auto previous_snapshot = table_info.table_metadata.GetLatestSnapshot();
	int64_t previous_snapshot_id = 0;
	if (previous_snapshot && previous_snapshot->snapshot_id) {
		previous_snapshot_id = *previous_snapshot->snapshot_id;
	}

	auto &iceberg_transaction = IcebergTransaction::Get(context, iceberg_table->catalog);
	ApplyTableUpdate(table_info, iceberg_transaction, [&](IcebergTable &tbl) {
		auto &transaction_data = tbl.GetOrCreateTransactionData(iceberg_transaction);
		transaction_data.TableRollbackToSnapshot(bind_data.snapshot_id);
	});

	FlatVector::GetDataMutable<int64_t>(output.data[0])[0] = previous_snapshot_id;
	FlatVector::GetDataMutable<int64_t>(output.data[1])[0] = bind_data.snapshot_id;
	output.SetChildCardinality(1);
	global_state.finished = true;
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergRollbackToSnapshotFunction() {
	TableFunctionSet function_set("iceberg_rollback_to_snapshot");
	auto fun = TableFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, IcebergRollbackToSnapshotFunction,
	                         IcebergRollbackToSnapshotBind, IcebergRollbackToSnapshotGlobalState::Init);
	function_set.AddFunction(fun);
	// iceberg_snapshots exposes snapshot_id as UBIGINT; accept that without requiring a cast.
	fun = TableFunction({LogicalType::VARCHAR, LogicalType::UBIGINT}, IcebergRollbackToSnapshotFunction,
	                    IcebergRollbackToSnapshotBind, IcebergRollbackToSnapshotGlobalState::Init);
	function_set.AddFunction(fun);
	return function_set;
}

} // namespace duckdb
