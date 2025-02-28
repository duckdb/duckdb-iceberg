#include "storage/irc_catalog.hpp"
#include "storage/irc_schema_entry.hpp"
#include "storage/irc_table_entry.hpp"
#include "storage/irc_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "catalog_api.hpp"
#include "../../duckdb/third_party/catch/catch.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"


namespace duckdb {

ICTableEntry::ICTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = false;
}

ICTableEntry::ICTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, ICTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info) {
	this->internal = false;
}

unique_ptr<BaseStatistics> ICTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void ICTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                         ClientContext &) {
	throw NotImplementedException("BindUpdateConstraints");
}

TableFunction ICTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &iceberg_scan_function_set = ExtensionUtil::GetTableFunction(db, "iceberg_scan");
	auto iceberg_scan_function = iceberg_scan_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});
	auto &ic_catalog = catalog.Cast<IRCatalog>();

	D_ASSERT(table_data);

	if (table_data->data_source_format != "ICEBERG") {
		throw NotImplementedException("Table '%s' is of unsupported format '%s', ", table_data->name,
		                              table_data->data_source_format);
	}

	auto &secret_manager = SecretManager::Get(context);
	
	// Get Credentials from IRC API
	auto table_credentials = IRCAPI::GetTableCredentials(
		ic_catalog.internal_name, table_data->schema_name, table_data->name, ic_catalog.credentials);
	// First check if table credentials are set (possible the IC catalog does not return credentials)
	if (!table_credentials.key_id.empty()) {
		// Inject secret into secret manager scoped to this path
		CreateSecretInfo info(OnCreateConflict::REPLACE_ON_CONFLICT, SecretPersistType::TEMPORARY);
		info.name = "__internal_ic_" + table_data->table_id;
		info.type = "s3";
		info.provider = "config";
		info.storage_type = "memory";
		info.options = {
			{"key_id", table_credentials.key_id},
			{"secret", table_credentials.secret},
			{"session_token", table_credentials.session_token},
			{"region", ic_catalog.credentials.aws_region},
		};

		std::string lc_storage_location;
		lc_storage_location.resize(table_data->storage_location.size());
		std::transform(table_data->storage_location.begin(), table_data->storage_location.end(), lc_storage_location.begin(), ::tolower);
		size_t metadata_pos = lc_storage_location.find("metadata");
		if (metadata_pos != std::string::npos) {
			info.scope = {lc_storage_location.substr(0, metadata_pos)};
		} else {
			throw std::runtime_error("Substring not found");
		}
		auto my_secret = secret_manager.CreateSecret(context, info);
	}

	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	// Set the S3 path as input to table function
	vector<Value> inputs = {table_data->storage_location};

	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr,
									  iceberg_scan_function, empty_ref);

	auto result = iceberg_scan_function.bind(context, bind_input, return_types, names);
	bind_data = std::move(result);

	return iceberg_scan_function;
}

TableStorageInfo ICTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	// TODO fill info
	return result;
}

} // namespace duckdb
