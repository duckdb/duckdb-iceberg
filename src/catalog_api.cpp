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

#include "rest_catalog/objects/list.hpp"

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

	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto load_table_result = rest_api_objects::LoadTableResult::FromJSON(root);
	catalog.SetCachedValue(url.GetURL(), api_result, load_table_result);
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
	auto cached_value = catalog.OptionalGetCachedValue(url.GetURL());
	if (!cached_value.empty()) {
		return cached_value;
	} else {
		return GetTableMetadata(context, catalog, schema, table);
	}
}

vector<string> IRCAPI::GetCatalogs(ClientContext &context, IRCatalog &catalog) {
	throw NotImplementedException("ICAPI::GetCatalogs");
}

static void ParseConfigOptions(const case_insensitive_map_t<string> &config, case_insensitive_map_t<Value> &options) {
	//! Set of recognized config parameters and the duckdb secret option that matches it.
	static const case_insensitive_map_t<string> config_to_option = {{"s3.access-key-id", "key_id"},
	                                                                {"s3.secret-access-key", "secret"},
	                                                                {"s3.session-token", "session_token"},
	                                                                {"s3.region", "region"},
	                                                                {"region", "region"},
	                                                                {"client.region", "region"},
	                                                                {"s3.endpoint", "endpoint"}};

	if (config.empty()) {
		return;
	}
	for (auto &entry : config) {
		auto it = config_to_option.find(entry.first);
		if (it != config_to_option.end()) {
			options[it->second] = entry.second;
		}
	}

	auto it = config.find("s3.path-style-access");
	if (it != config.end()) {
		bool path_style;
		if (it->second == "true") {
			path_style = true;
		} else if (it->second == "false") {
			path_style = false;
		} else {
			throw InvalidInputException("Unexpected value ('%s') for 's3.path-style-access' in 'config' property",
			                            it->second);
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
	auto load_table_result = rest_api_objects::LoadTableResult::FromJSON(root);

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

	if (load_table_result.has_config) {
		auto &config = load_table_result.config;
		ParseConfigOptions(config, config_options);
	}

	if (load_table_result.has_storage_credentials) {
		auto &storage_credentials = load_table_result.storage_credentials;
		for (idx_t index = 0; index < storage_credentials.size(); index++) {
			auto &credential = storage_credentials[index];
			CreateSecretInput create_secret_input;
			create_secret_input.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
			create_secret_input.persist_type = SecretPersistType::TEMPORARY;

			create_secret_input.scope.push_back(credential.prefix);
			create_secret_input.name = StringUtil::Format("%s_%d_%s", secret_base_name, index, credential.prefix);
			create_secret_input.type = "s3";
			create_secret_input.provider = "config";
			create_secret_input.storage_type = "memory";
			create_secret_input.options = config_options;

			ParseConfigOptions(credential.config, create_secret_input.options);
			//! TODO: apply the 'overrides' retrieved from the /v1/config endpoint
			result.storage_credentials.push_back(create_secret_input);
		}
	}

	if (result.storage_credentials.empty() && !config_options.empty()) {
		//! Only create a secret out of the 'config' if there are no 'storage-credentials'
		result.config = make_uniq<CreateSecretInput>();
		auto &config = *result.config;
		config.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
		config.persist_type = SecretPersistType::TEMPORARY;

		//! TODO: apply the 'overrides' retrieved from the /v1/config endpoint
		config.options = config_options;
		config.name = secret_base_name;
		config.type = "s3";
		config.provider = "config";
		config.storage_type = "memory";
	}

	return result;
}

static void populateTableMetadata(IRCAPITable &table, const rest_api_objects::LoadTableResult &load_table_result) {
	if (!load_table_result.has_metadata_location) {
		throw NotImplementedException(
		    "metadata-location is expected to be populated, likely missing support for V1 Iceberg");
	}
	table.storage_location = load_table_result.metadata_location;
	auto &metadata = load_table_result.metadata;
	// table_result.table_id = load_table_result.metadata.table_uuid;

	//! NOTE: these (schema-id and current-schema-id) are saved as int64_t in the parsed structure,
	//! the spec lists them as 'int', so I think they are really int32_t. (?)
	//! We parsed them as uint64_t before using the generated JSON->CPP parsing logic.
	bool found = false;
	if (!metadata.has_current_schema_id) {
		//! It's required since v2, but we want to support reading v1 as well, no?
		throw NotImplementedException("FIXME: We require the 'current-schema-id' always, the spec says it's optional?");
		//! FIXME: for v1 we should check if `schema` is set and use that instead
	}
	int64_t current_schema_id = metadata.current_schema_id;
	if (!metadata.has_schemas) {
		throw NotImplementedException("'schemas' is not present! V1 not supported currently");
		//! FIXME: for v1 we should check if `schema` is set and use that instead (see above)
	}
	for (auto &schema : metadata.schemas) {
		auto &schema_internals = schema.object_1;
		if (!schema_internals.has_schema_id) {
			throw NotImplementedException("'schema-id' not present! V1 not supported currently!");
		}
		int64_t schema_id = schema_internals.schema_id;
		if (schema_id == current_schema_id) {
			found = true;
			auto &columns = schema.struct_type.fields;
			for (auto &col : columns) {
				table.columns.push_back(IcebergColumnDefinition::ParseStructField(*col));
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
		auto load_table_result = rest_api_objects::LoadTableResult::FromJSON(metadata_root);
		populateTableMetadata(table_result, load_table_result);
	} else {
		// Skip fetching metadata, we'll do it later when we access the table
		auto col = make_uniq<IcebergColumnDefinition>();
		col->name = "__";
		col->id = 0;
		col->type = LogicalType::UNKNOWN;
		table_result.columns.push_back(std::move(col));
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
	auto list_tables_response = rest_api_objects::ListTablesResponse::FromJSON(root);

	if (!list_tables_response.has_identifiers) {
		throw NotImplementedException("List of 'identifiers' is missing, missing support for Iceberg V1");
	}
	for (auto &table : list_tables_response.identifiers) {
		auto table_result = GetTable(context, catalog, schema, table.name);
		result.push_back(std::move(table_result));
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
	auto list_namespaces_response = rest_api_objects::ListNamespacesResponse::FromJSON(root);
	if (!list_namespaces_response.has_namespaces) {
		//! FIXME: old code expected 'namespaces' to always be present, but it's not a required property
		return result;
	}
	auto &schemas = list_namespaces_response.namespaces;
	for (auto &schema : schemas) {
		IRCAPISchema schema_result;
		schema_result.catalog_name = catalog.GetName();
		auto &value = schema.value;
		if (value.size() != 1) {
			//! FIXME: we likely want to fix this by concatenating the components with a `.` ?
			throw NotImplementedException("Only a namespace with a single component is supported currently, found %d",
			                              value.size());
		}
		schema_result.schema_name = value[0];
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
