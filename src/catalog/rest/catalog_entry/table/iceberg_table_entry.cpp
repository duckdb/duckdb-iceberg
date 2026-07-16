#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "catalog/rest/storage/authorization/sigv4.hpp"
#include "rest_catalog/objects/list.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "common/iceberg_default.hpp"

namespace duckdb {
class OAuth2Authorization;
constexpr column_t IcebergMultiFileReader::COLUMN_IDENTIFIER_LAST_SEQUENCE_NUMBER;

IcebergTableEntry::IcebergTableEntry(IcebergTableInformation &table_info, Catalog &catalog, SchemaCatalogEntry &schema,
                                     CreateTableInfo &info, optional_idx schema_id)
    : TableCatalogEntry(catalog, schema, info), table_info(table_info), schema_id(schema_id) {
	this->internal = false;
}

unique_ptr<BaseStatistics> IcebergTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void IcebergTableEntry::PrepareIcebergScanFromEntry(ClientContext &context) const {
	table_info.LoadCredentials(context);
}

TableFunction IcebergTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                 const EntryLookupInfo &lookup) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &catalog_schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto catalog_entry = catalog_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "iceberg_scan");
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"iceberg_scan\" not found!");
	}
	auto &iceberg_scan_function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	auto iceberg_scan_function =
	    iceberg_scan_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});
	PrepareIcebergScanFromEntry(context);

	if (!schema_id.IsValid()) {
		throw InternalException("GetScanFunction was called with a dummy IcebergTableEntry, this should never happen");
	}
	const auto schema_id = this->schema_id.GetIndex();
	const auto &metadata = table_info.table_metadata;
	const auto &iceberg_schema = *metadata.GetSchemaFromId(schema_id);

	// The pinned metadata object's current snapshot is the transaction-visible head. Only an explicit AT clause
	// selects a different snapshot.
	auto snapshot_lookup = IcebergSnapshotLookup::FromAtClause(lookup.GetAtClause());

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info = metadata.GetSnapshot(snapshot_lookup);
	//! Override whatever schema id the lookup resulted in
	//! The schema is preset by the IcebergCatalogEntry and we can not deviate from that
	snapshot_info.schema_id = schema_id;

	auto &fs = FileSystem::GetFileSystem(context);
	auto scan_info =
	    make_shared_ptr<IcebergScanInfo>(metadata.GetMetadataPath(fs), metadata, snapshot_info, iceberg_schema);
	if (table_info.transaction_data && snapshot_lookup.IsLatest()) {
		scan_info->transaction_data = table_info.transaction_data.get();
	}

	iceberg_scan_function.function_info = scan_info;
	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<Identifier> names;
	TableFunctionRef empty_ref;

	// Set the S3 path as input to table function
	const auto &storage_location = metadata.location;
	vector<Value> inputs = {storage_location};
	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, iceberg_scan_function,
	                                  empty_ref);
	vector<string> bind_names;
	auto result = iceberg_scan_function.bind(context, bind_input, return_types, bind_names);
	names = StringsToIdentifiers(bind_names);
	bind_data = std::move(result);
	auto &file_bind_data = bind_data->Cast<MultiFileBindData>();
	file_bind_data.virtual_columns = GetVirtualColumns();
	D_ASSERT(file_bind_data.file_list);
	auto &ic_file_list = file_bind_data.file_list->Cast<IcebergMultiFileList>();
	ic_file_list.SetTable(this);
	return iceberg_scan_function;
}

TableFunction IcebergTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	throw InternalException("IcebergTableEntry::GetScanFunction called without entry lookup info");
}

virtual_column_map_t IcebergTableEntry::GetVirtualColumns() const {
	return VirtualColumns();
}

virtual_column_map_t IcebergTableEntry::VirtualColumns() {
	virtual_column_map_t result;
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR));
	result.emplace(COLUMN_IDENTIFIER_ROW_ID, TableColumn("_row_id", LogicalType::BIGINT));
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	               TableColumn("file_row_number", LogicalType::BIGINT));
	result.emplace(IcebergMultiFileReader::COLUMN_IDENTIFIER_LAST_SEQUENCE_NUMBER,
	               TableColumn("_last_updated_sequence_number", LogicalType::BIGINT));
	return result;
}

vector<column_t> IcebergTableEntry::GetRowIdColumns() const {
	vector<column_t> result;
	auto &table_metadata = table_info.table_metadata;
	if (table_metadata.iceberg_version >= 3) {
		//! Project the _row_id column as part of the row-id-columns
		result.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	result.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILENAME);
	result.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
	return result;
}

LogicalType IcebergTableEntry::GetExpectedTypeForInsert(const ColumnDefinition &column) const {
	auto type = column.Type();
	if (type.id() == LogicalTypeId::STRUCT) {
		return LogicalType::INVALID;
	}
	return type;
}

unique_ptr<Expression> IcebergTableEntry::GetDefaultExpressionForColumn(ClientContext &context,
                                                                        const LogicalType &input_type,
                                                                        const LogicalType &result_type,
                                                                        ColumnBinding binding,
                                                                        const Expression &constant_value) const {
	return IcebergDefaultProjectionResolver::ResolveDefault(context, input_type, result_type, binding, constant_value);
}

TableStorageInfo IcebergTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	// TODO fill info
	return result;
}

string IcebergTableEntry::GetUUID() const {
	return table_info.table_id;
}

} // namespace duckdb
