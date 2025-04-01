#include "storage/irc_catalog.hpp"
#include "storage/irc_schema_entry.hpp"
#include "storage/irc_table_entry.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "catalog_api.hpp"
#include "duckdb/planner/binder.hpp"
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
	auto iceberg_scan_function =
	    iceberg_scan_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});
	auto &ic_catalog = catalog.Cast<IRCatalog>();

	D_ASSERT(table_data);

	if (table_data->data_source_format != "ICEBERG") {
		throw NotImplementedException("Table '%s' is of unsupported format '%s', ", table_data->name,
		                              table_data->data_source_format);
	}

	auto &secret_manager = SecretManager::Get(context);

	// Get Credentials from IRC API
	auto secret_base_name =
	    StringUtil::Format("__internal_ic_%s__%s__%s", table_data->table_id, table_data->schema_name, table_data->name);
	auto table_credentials =
	    IRCAPI::GetTableCredentials(context, ic_catalog, table_data->schema_name, table_data->name, secret_base_name);
	CreateSecretInfo info(OnCreateConflict::REPLACE_ON_CONFLICT, SecretPersistType::TEMPORARY);
	// First check if table credentials are set (possible the IC catalog does not return credentials)

	if (table_credentials.config) {
		auto &info = *table_credentials.config;
		D_ASSERT(info.scope.empty());
		//! Limit the scope to the metadata location
		std::string lc_storage_location;
		lc_storage_location.resize(table_data->storage_location.size());
		std::transform(table_data->storage_location.begin(), table_data->storage_location.end(),
		               lc_storage_location.begin(), ::tolower);
		size_t metadata_pos = lc_storage_location.find("metadata");
		if (metadata_pos != std::string::npos) {
			info.scope = {lc_storage_location.substr(0, metadata_pos)};
		} else {
			throw InvalidInputException("Substring not found");
		}

		if (StringUtil::StartsWith(ic_catalog.host, "glue")) {
			//! Override the endpoint if 'glue' is the host of the catalog
			auto secret_entry = IRCatalog::GetSecret(context, ic_catalog.secret_name);
			auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
			auto region = kv_secret.TryGetValue("region").ToString();
			auto endpoint = "s3." + region + ".amazonaws.com";
			info.options["endpoint"] = endpoint;
		} else if (StringUtil::StartsWith(ic_catalog.host, "s3tables")) {
			//! Override all the options if 's3tables' is the host of the catalog
			auto secret_entry = IRCatalog::GetSecret(context, ic_catalog.secret_name);
			auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
			auto substrings = StringUtil::Split(ic_catalog.warehouse, ":");
			D_ASSERT(substrings.size() == 6);
			auto region = substrings[3];
			auto endpoint = "s3." + region + ".amazonaws.com";
			info.options = {{"key_id", kv_secret.TryGetValue("key_id").ToString()},
			                {"secret", kv_secret.TryGetValue("secret").ToString()},
			                {"session_token", kv_secret.TryGetValue("session_token").IsNull()
			                                      ? ""
			                                      : kv_secret.TryGetValue("session_token").ToString()},
			                {"region", region},
			                {"endpoint", endpoint}};
		}
		(void)secret_manager.CreateSecret(context, info);
	}

	for (auto &info : table_credentials.storage_credentials) {
		(void)secret_manager.CreateSecret(context, info);
	}

	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	// Set the S3 path as input to table function
	vector<Value> inputs = {table_data->storage_location};

	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, iceberg_scan_function,
	                                  empty_ref);

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
