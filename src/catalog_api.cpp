#include "catalog_api.hpp"
#include "catalog_utils.hpp"
#include "storage/irc_catalog.hpp"
#include "yyjson.hpp"
#include "iceberg_utils.hpp"
#include "api_utils.hpp"
#include <sys/stat.h>
#include <duckdb/main/secret/secret.hpp>
#include <duckdb/main/secret/secret_manager.hpp>
#include "duckdb/common/error_data.hpp"
#include "storage/authorization/sigv4.hpp"
#include "storage/authorization/oauth2.hpp"
#include "curl.hpp"

using namespace duckdb_yyjson;
namespace duckdb {

static string GetTableMetadata(ClientContext &context, IRCatalog &catalog, const string &schema, const string &table) {
	RequestInput request_input;

	auto url = catalog.GetBaseUrl();
	url.AddPathComponent(catalog.prefix);
	url.AddPathComponent("namespaces");
	url.AddPathComponent(schema);
	url.AddPathComponent("tables");
	url.AddPathComponent(table);

	request_input.AddHeader("X-Iceberg-Access-Delegation: vended-credentials");
	string api_result = catalog.auth_handler->GetRequest(context, url, request_input);

	catalog.SetCachedValue(url.GetURL(), api_result);
	return api_result;
}

static string GetTableMetadataCached(ClientContext &context, IRCatalog &catalog, const string &schema,
                                     const string &table) {
	auto url = catalog.GetBaseUrl();
	url.AddPathComponent(catalog.prefix);
	url.AddPathComponent("namespaces");
	url.AddPathComponent(schema);
	url.AddPathComponent("tables");
	url.AddPathComponent(table);
	if (catalog.HasCachedValue(url.GetURL())) {
		return catalog.GetCachedValue(url.GetURL());
	}
	return GetTableMetadata(context, catalog, schema, table);
}

vector<string> IRCAPI::GetCatalogs(ClientContext &context, IRCatalog &catalog) {
	throw NotImplementedException("ICAPI::GetCatalogs");
}

static IRCAPIColumnDefinition ParseColumnDefinition(yyjson_val *column_def) {
	IRCAPIColumnDefinition result;
	result.name = IcebergUtils::TryGetStrFromObject(column_def, "name");
	result.type_text = IcebergUtils::TryGetStrFromObject(column_def, "type");
	result.precision =
	    (result.type_text == "decimal") ? IcebergUtils::TryGetNumFromObject(column_def, "type_precision") : -1;
	result.scale = (result.type_text == "decimal") ? IcebergUtils::TryGetNumFromObject(column_def, "type_scale") : -1;
	result.position = IcebergUtils::TryGetNumFromObject(column_def, "id") - 1;
	return result;
}

static void ParseConfigOptions(yyjson_val *config, case_insensitive_map_t<Value> &options) {
	//! Set of recognized config parameters and the duckdb secret option that matches it.
	static const case_insensitive_map_t<string> config_to_option = {{"s3.access-key-id", "key_id"},
	                                                                {"s3.secret-access-key", "secret"},
	                                                                {"s3.session-token", "session_token"},
	                                                                {"s3.region", "region"},
	                                                                {"region", "region"},
	                                                                {"client.region", "region"},
	                                                                {"s3.endpoint", "endpoint"}};

	auto config_size = yyjson_obj_size(config);
	if (!config || config_size == 0) {
		return;
	}
	for (auto &it : config_to_option) {
		auto &key = it.first;
		auto &option = it.second;

		auto *item = yyjson_obj_get(config, key.c_str());
		if (item) {
			options[option] = yyjson_get_str(item);
		}
	}
	auto *access_style = yyjson_obj_get(config, "s3.path-style-access");
	if (access_style) {
		string value = yyjson_get_str(access_style);
		bool path_style;
		if (value == "true") {
			path_style = true;
		} else if (value == "false") {
			path_style = false;
		} else {
			throw InvalidInputException("Unexpected value ('%s') for 's3.path-style-access' in 'config' property",
			                            value);
		}
		options["use_ssl"] = Value(!path_style);
		if (path_style) {
			options["url_style"] = "path";
		}
	}

	auto endpoint_it = options.find("endpoint");
	if (endpoint_it == options.end()) {
		return;
	}
	auto endpoint = endpoint_it->second.ToString();
	if (StringUtil::StartsWith(endpoint, "http://")) {
		endpoint = endpoint.substr(7, std::string::npos);
	}
	if (StringUtil::StartsWith(endpoint, "https://")) {
		endpoint = endpoint.substr(8, std::string::npos);
	}
	if (StringUtil::EndsWith(endpoint, "/")) {
		endpoint = endpoint.substr(0, endpoint.size() - 1);
	}
	endpoint_it->second = endpoint;
}

IRCAPITableCredentials IRCAPI::GetTableCredentials(ClientContext &context, IRCatalog &catalog, const string &schema,
                                                   const string &table, const string &secret_base_name) {
	IRCAPITableCredentials result;
	string api_result = GetTableMetadataCached(context, catalog, schema, table);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());

	case_insensitive_map_t<Value> user_defaults;
	if (catalog.auth_handler->type == IRCAuthorizationType::SIGV4) {
		auto &sigv4_auth = catalog.auth_handler->Cast<SIGV4Authorization>();
		auto catalog_credentials = IRCatalog::GetStorageSecret(context, sigv4_auth.secret);
		// start with the credentials needed for the catalog and overwrite information contained
		// in the vended credentials. We do it this way to maintain the region info from the catalog credentials
		if (catalog_credentials) {
			auto kv_secret = dynamic_cast<const KeyValueSecret &>(*catalog_credentials->secret);
			for (auto &option : kv_secret.secret_map) {
				// Ignore refresh info.
				// if the credentials are the same as for the catalog, then refreshing the catalog secret is enough
				// otherwise the vended credentials contain their own information for refreshing.
				if (option.first != "refresh_info" && option.first != "refresh") {
					user_defaults.emplace(option);
				}
			}
		}
	} else if (catalog.auth_handler->type == IRCAuthorizationType::OAUTH2) {
		auto &oauth2_auth = catalog.auth_handler->Cast<OAuth2Authorization>();
		if (!oauth2_auth.default_region.empty()) {
			user_defaults["region"] = oauth2_auth.default_region;
		}
	}

	// Mapping from config key to a duckdb secret option

	case_insensitive_map_t<Value> config_options;
	//! TODO: apply the 'defaults' retrieved from the /v1/config endpoint
	config_options.insert(user_defaults.begin(), user_defaults.end());

	auto *config_val = yyjson_obj_get(root, "config");
	ParseConfigOptions(config_val, config_options);

	auto *storage_credentials = yyjson_obj_get(root, "storage-credentials");
	auto storage_credentials_size = yyjson_arr_size(storage_credentials);
	if (storage_credentials && storage_credentials_size > 0) {
		yyjson_val *storage_credential;
		size_t index, max;
		yyjson_arr_foreach(storage_credentials, index, max, storage_credential) {
			auto *sc_prefix = yyjson_obj_get(storage_credential, "prefix");
			if (!sc_prefix) {
				throw InvalidInputException("required property 'prefix' is missing from the StorageCredential schema");
			}

			CreateSecretInfo create_secret_input(OnCreateConflict::REPLACE_ON_CONFLICT, SecretPersistType::TEMPORARY);

			auto prefix_string = yyjson_get_str(sc_prefix);
			if (!prefix_string) {
				throw InvalidInputException("property 'prefix' of StorageCredential is NULL");
			}
			create_secret_input.scope.push_back(string(prefix_string));
			create_secret_input.name = StringUtil::Format("%s_%d_%s", secret_base_name, index, prefix_string);
			create_secret_input.type = "s3";
			create_secret_input.provider = "config";
			create_secret_input.storage_type = "memory";
			create_secret_input.options = config_options;

			auto *sc_config = yyjson_obj_get(storage_credential, "config");
			ParseConfigOptions(sc_config, create_secret_input.options);
			//! TODO: apply the 'overrides' retrieved from the /v1/config endpoint
			result.storage_credentials.push_back(create_secret_input);
		}
	}

	if (result.storage_credentials.empty() && !config_options.empty()) {
		//! Only create a secret out of the 'config' if there are no 'storage-credentials'
		result.config =
		    make_uniq<CreateSecretInfo>(OnCreateConflict::REPLACE_ON_CONFLICT, SecretPersistType::TEMPORARY);
		auto &config = *result.config;

		//! TODO: apply the 'overrides' retrieved from the /v1/config endpoint
		config.options = config_options;
		config.name = secret_base_name;
		config.type = "s3";
		config.provider = "config";
		config.storage_type = "memory";
	}

	return result;
}

static void populateTableMetadata(IRCAPITable &table, yyjson_val *metadata_root) {
	table.storage_location = IcebergUtils::TryGetStrFromObject(metadata_root, "metadata-location");
	auto *metadata = yyjson_obj_get(metadata_root, "metadata");
	// table_result.table_id = IcebergUtils::TryGetStrFromObject(metadata, "table-uuid");

	uint64_t current_schema_id = IcebergUtils::TryGetNumFromObject(metadata, "current-schema-id");
	auto *schemas = yyjson_obj_get(metadata, "schemas");
	yyjson_val *schema;
	size_t schema_idx, schema_max;
	bool found = false;
	yyjson_arr_foreach(schemas, schema_idx, schema_max, schema) {
		uint64_t schema_id = IcebergUtils::TryGetNumFromObject(schema, "schema-id");
		if (schema_id == current_schema_id) {
			found = true;
			auto *columns = yyjson_obj_get(schema, "fields");
			yyjson_val *col;
			size_t col_idx, col_max;
			yyjson_arr_foreach(columns, col_idx, col_max, col) {
				auto column_definition = ParseColumnDefinition(col);
				table.columns.push_back(column_definition);
			}
		}
	}

	if (!found) {
		throw InvalidInputException("Current schema not found");
	}
}

static IRCAPITable createTable(IRCatalog &catalog, const string &schema, const string &table_name) {
	IRCAPITable table_result;
	table_result.catalog_name = catalog.GetName();
	table_result.schema_name = schema;
	table_result.name = table_name;
	table_result.data_source_format = "ICEBERG";
	table_result.table_id = "uuid-" + schema + "-" + "table";
	std::replace(table_result.table_id.begin(), table_result.table_id.end(), '_', '-');
	return table_result;
}

IRCAPITable IRCAPI::GetTable(ClientContext &context, IRCatalog &catalog, const string &schema, const string &table_name,
                             bool perform_request) {
	IRCAPITable table_result = createTable(catalog, schema, table_name);

	if (perform_request) {
		string result = GetTableMetadata(context, catalog, schema, table_result.name);
		std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(result));
		auto *metadata_root = yyjson_doc_get_root(doc.get());
		populateTableMetadata(table_result, metadata_root);
	} else {
		// Skip fetching metadata, we'll do it later when we access the table
		IRCAPIColumnDefinition col;
		col.name = "__";
		col.type_text = "int";
		col.precision = -1;
		col.scale = -1;
		col.position = 0;
		table_result.columns.push_back(col);
	}

	return table_result;
}

// TODO: handle out-of-order columns using position property
vector<IRCAPITable> IRCAPI::GetTables(ClientContext &context, IRCatalog &catalog, const string &schema) {
	vector<IRCAPITable> result;
	auto url = catalog.GetBaseUrl();
	url.AddPathComponent(catalog.prefix);
	url.AddPathComponent("namespaces");
	url.AddPathComponent(schema);
	url.AddPathComponent("tables");
	RequestInput request_input;
	string api_result = catalog.auth_handler->GetRequest(context, url, request_input);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto *tables = yyjson_obj_get(root, "identifiers");
	size_t idx, max;
	yyjson_val *table;
	yyjson_arr_foreach(tables, idx, max, table) {
		auto table_result = GetTable(context, catalog, schema, IcebergUtils::TryGetStrFromObject(table, "name"));
		result.push_back(table_result);
	}

	return result;
}

vector<IRCAPISchema> IRCAPI::GetSchemas(ClientContext &context, IRCatalog &catalog) {
	vector<IRCAPISchema> result;
	auto endpoint_builder = catalog.GetBaseUrl();
	endpoint_builder.AddPathComponent(catalog.prefix);
	endpoint_builder.AddPathComponent("namespaces");
	RequestInput request_input;
	string api_result = catalog.auth_handler->GetRequest(context, endpoint_builder, request_input);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	//! 'ListNamespacesResponse'
	auto *schemas = yyjson_obj_get(root, "namespaces");
	size_t idx, max;
	yyjson_val *schema;
	yyjson_arr_foreach(schemas, idx, max, schema) {
		IRCAPISchema schema_result;
		schema_result.catalog_name = catalog.GetName();
		yyjson_val *value = yyjson_arr_get(schema, 0);
		schema_result.schema_name = yyjson_get_str(value);
		result.push_back(schema_result);
	}

	return result;
}

IRCAPISchema IRCAPI::CreateSchema(ClientContext &context, IRCatalog &catalog, const string &internal,
                                  const string &schema) {
	throw NotImplementedException("IRCAPI::Create Schema not Implemented");
}

void IRCAPI::DropSchema(ClientContext &context, const string &internal, const string &schema) {
	throw NotImplementedException("IRCAPI Drop Schema not Implemented");
}

void IRCAPI::DropTable(ClientContext &context, IRCatalog &catalog, const string &internal, const string &schema,
                       string &table_name) {
	throw NotImplementedException("IRCAPI Drop Table not Implemented");
}

static std::string json_to_string(yyjson_mut_doc *doc, yyjson_write_flag flags = YYJSON_WRITE_PRETTY) {
	char *json_chars = yyjson_mut_write(doc, flags, NULL);
	std::string json_str(json_chars);
	free(json_chars);
	return json_str;
}

IRCAPITable IRCAPI::CreateTable(ClientContext &context, IRCatalog &catalog, const string &internal,
                                const string &schema, CreateTableInfo *table_info) {
	throw NotImplementedException("IRCAPI Create Table not Implemented");
}

} // namespace duckdb
