#include "catalog_api.hpp"
#include "catalog_utils.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_schema_entry.hpp"
#include "yyjson.hpp"
#include "iceberg_utils.hpp"
#include "api_utils.hpp"
#include <sys/stat.h>
#include <duckdb/main/secret/secret.hpp>
#include <duckdb/main/secret/secret_manager.hpp>
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"

#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;
namespace duckdb {

vector<string> IRCAPI::ParseSchemaName(string &namespace_name) {
	idx_t start = 0;
	idx_t end = namespace_name.find(".", start);
	vector<string> ret;
	while (end != std::string::npos) {
		auto nested_identifier = namespace_name.substr(start, end - start);
		ret.push_back(nested_identifier);
		start = end + 1;
		end = namespace_name.find(".", start);
	}
	auto last_identifier = namespace_name.substr(start, end - start);
	ret.push_back(last_identifier);
	return ret;
}

//! Used for the query parameter (parent=...)
string IRCAPI::GetSchemaName(const vector<string> &items) {
	static const string unit_separator = "\x1F";
	return StringUtil::Join(items, unit_separator);
}

//! Used for the path parameters
string IRCAPI::GetEncodedSchemaName(const vector<string> &items) {
	static const string unit_separator = "%1F";
	return StringUtil::Join(items, unit_separator);
}

[[noreturn]] static void ThrowException(const string &url, const HTTPResponse &response, const string &method) {
	D_ASSERT(!response.Success());

	if (response.HasRequestError()) {
		//! Request error - this means something went wrong performing the request
		throw IOException("%s request to endpoint '%s' failed: (ERROR %s)", method, url, response.GetRequestError());
	}
	//! FIXME: the spec defines response objects for all failure conditions, we can deserialize the response and
	//! return a more descriptive error message based on that.
	if (!response.reason.empty()) {
		throw HTTPException(response, "%s request to endpoint '%s' returned an error response (HTTP %n). Reason: %s",
		                    method, url, int(response.status), response.reason);
	}

	//! If this was not a request error this means the server responded - report the response status and response
	throw HTTPException(response, "%s request to endpoint '%s' returned an error response (HTTP %n)", method, url,
	                    int(response.status));
}

static string GetTableMetadata(ClientContext &context, IRCatalog &catalog, const IRCSchemaEntry &schema,
                               const string &table) {
	auto schema_name = IRCAPI::GetEncodedSchemaName(schema.namespace_items);

	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");
	url_builder.AddPathComponent(schema_name);
	url_builder.AddPathComponent("tables");
	url_builder.AddPathComponent(table);

	auto url = url_builder.GetURL();
	auto response = catalog.auth_handler->GetRequest(context, url_builder);
	if (!response->Success()) {
		ThrowException(url, *response, "GET");
	}

	const auto &api_result = response->body;
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto load_table_result = rest_api_objects::LoadTableResult::FromJSON(root);
	catalog.SetCachedValue(url, api_result, load_table_result);
	return api_result;
}

rest_api_objects::LoadTableResult IRCAPI::GetTable(ClientContext &context, IRCatalog &catalog,
                                                   const IRCSchemaEntry &schema, const string &table_name) {
	string result = GetTableMetadata(context, catalog, schema, table_name);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(result));
	auto *metadata_root = yyjson_doc_get_root(doc.get());
	auto load_table_result = rest_api_objects::LoadTableResult::FromJSON(metadata_root);
	return load_table_result;
}

vector<rest_api_objects::TableIdentifier> IRCAPI::GetTables(ClientContext &context, IRCatalog &catalog,
                                                            const IRCSchemaEntry &schema) {
	auto schema_name = GetEncodedSchemaName(schema.namespace_items);

	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");
	url_builder.AddPathComponent(schema_name);
	url_builder.AddPathComponent("tables");
	auto response = catalog.auth_handler->GetRequest(context, url_builder);
	if (!response->Success()) {
		auto url = url_builder.GetURL();
		ThrowException(url, *response, "GET");
	}

	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(response->body));
	auto *root = yyjson_doc_get_root(doc.get());
	auto list_tables_response = rest_api_objects::ListTablesResponse::FromJSON(root);

	if (!list_tables_response.has_identifiers) {
		throw NotImplementedException("List of 'identifiers' is missing, missing support for Iceberg V1");
	}
	return std::move(list_tables_response.identifiers);
}

vector<IRCAPISchema> IRCAPI::GetSchemas(ClientContext &context, IRCatalog &catalog, const vector<string> &parent) {
	vector<IRCAPISchema> result;
	auto endpoint_builder = catalog.GetBaseUrl();
	endpoint_builder.AddPathComponent(catalog.prefix);
	endpoint_builder.AddPathComponent("namespaces");
	if (!parent.empty()) {
		auto parent_name = GetSchemaName(parent);
		endpoint_builder.SetParam("parent", parent_name);
	}
	auto response = catalog.auth_handler->GetRequest(context, endpoint_builder);
	if (!response->Success()) {
		auto url = endpoint_builder.GetURL();
		ThrowException(url, *response, "GET");
	}

	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(response->body));
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
		schema_result.items = std::move(schema.value);

		if (catalog.attach_options.support_nested_namespaces) {
			auto new_parent = parent;
			new_parent.push_back(schema_result.items.back());
			auto nested_namespaces = GetSchemas(context, catalog, new_parent);
			result.insert(result.end(), std::make_move_iterator(nested_namespaces.begin()),
			              std::make_move_iterator(nested_namespaces.end()));
		}

		result.push_back(schema_result);
	}
	return result;
}

void IRCAPI::CommitMultiTableUpdate(ClientContext &context, IRCatalog &catalog, const string &body) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("transactions");
	url_builder.AddPathComponent("commit");

	auto response = catalog.auth_handler->PostRequest(context, url_builder, body);
	if (response->status != HTTPStatusCode::OK_200 && response->status != HTTPStatusCode::NoContent_204) {
		throw InvalidConfigurationException(
		    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s", url_builder.GetURL(),
		    EnumUtil::ToString(response->status), response->reason, response->body);
	}
}

void IRCAPI::CommitTableUpdate(ClientContext &context, IRCatalog &catalog, const vector<string> &schema,
                               const string &table_name, const string &body) {
	auto schema_name = GetEncodedSchemaName(schema);

	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");
	url_builder.AddPathComponent(schema_name);
	url_builder.AddPathComponent("tables");
	url_builder.AddPathComponent(table_name);

	auto response = catalog.auth_handler->PostRequest(context, url_builder, body);
	if (response->status != HTTPStatusCode::OK_200 && response->status != HTTPStatusCode::NoContent_204) {
		throw InvalidConfigurationException(
		    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s", url_builder.GetURL(),
		    EnumUtil::ToString(response->status), response->reason, response->body);
	}
}

void IRCAPI::CommitTableDelete(ClientContext &context, IRCatalog &catalog, const vector<string> &schema,
                               const string &table_name) {
	auto schema_name = GetEncodedSchemaName(schema);
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");
	url_builder.AddPathComponent(schema_name);
	url_builder.AddPathComponent("tables");
	url_builder.AddPathComponent(table_name);
	url_builder.SetParam("purgeRequested", Value::BOOLEAN(catalog.attach_options.purge_requested).ToString());

	auto response = catalog.auth_handler->DeleteRequest(context, url_builder);
	// Glue/S3Tables follow spec and return 204, apache/iceberg-rest-fixture docker image returns 200
	if (response->status != HTTPStatusCode::NoContent_204 && response->status != HTTPStatusCode::OK_200) {
		throw InvalidConfigurationException(
		    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s", url_builder.GetURL(),
		    EnumUtil::ToString(response->status), response->reason, response->body);
	}
}

void IRCAPI::CommitNamespaceCreate(ClientContext &context, IRCatalog &catalog, string body) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");

	auto response = catalog.auth_handler->PostRequest(context, url_builder, body);
	if (response->status != HTTPStatusCode::OK_200) {
		throw InvalidConfigurationException(
		    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s", url_builder.GetURL(),
		    EnumUtil::ToString(response->status), response->reason, response->body);
	}
}

void IRCAPI::CommitNamespaceDrop(ClientContext &context, IRCatalog &catalog, vector<string> namespace_items) {
	auto url_builder = catalog.GetBaseUrl();
	auto schema_name = GetEncodedSchemaName(namespace_items);
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");
	url_builder.AddPathComponent(schema_name);

	auto response = catalog.auth_handler->DeleteRequest(context, url_builder);
	// Glue/S3Tables follow spec and return 204, apache/iceberg-rest-fixture docker image returns 200
	if (response->status != HTTPStatusCode::NoContent_204 && response->status != HTTPStatusCode::OK_200) {
		throw InvalidConfigurationException(
		    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s", url_builder.GetURL(),
		    EnumUtil::ToString(response->status), response->reason, response->body);
	}
}

rest_api_objects::LoadTableResult IRCAPI::CommitNewTable(ClientContext &context, IRCatalog &catalog,
                                                         const ICTableEntry *table) {
	auto &ic_catalog = table->catalog.Cast<IRCatalog>();
	auto &ic_schema = table->schema.Cast<IRCSchemaEntry>();
	auto table_namespace = GetEncodedSchemaName(ic_schema.namespace_items);
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");
	url_builder.AddPathComponent(table_namespace);
	url_builder.AddPathComponent("tables");

	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	yyjson_mut_doc *doc = doc_p.get();
	auto root_object = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_object);

	auto initial_schema = table->table_info.table_metadata.schemas[table->table_info.table_metadata.current_schema_id];
	auto create_transaction = make_uniq<IcebergCreateTableRequest>(initial_schema, table->table_info.name);
	// if stage create is supported, create the table with stage_create = true and the table update will
	// commit the table.
	auto support_stage_create = catalog.attach_options.supports_stage_create;
	yyjson_mut_obj_add_bool(doc, root_object, "stage-create", support_stage_create);
	auto create_table_json = create_transaction->CreateTableToJSON(std::move(doc_p));

	try {
		auto response = catalog.auth_handler->PostRequest(context, url_builder, create_table_json);
		if (response->status != HTTPStatusCode::OK_200) {
			throw InvalidConfigurationException(
			    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s", url_builder.GetURL(),
			    EnumUtil::ToString(response->status), response->reason, response->body);
		}
		std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(response->body));
		auto *root = yyjson_doc_get_root(doc.get());
		auto load_table_result = rest_api_objects::LoadTableResult::FromJSON(root);
		return load_table_result;
	} catch (const std::exception &e) {
		throw InvalidConfigurationException("Request to '%s' returned a non-200 status code body: %s",
		                                    url_builder.GetURL(), e.what());
	}
}

rest_api_objects::CatalogConfig IRCAPI::GetCatalogConfig(ClientContext &context, IRCatalog &catalog) {
	auto url = catalog.GetBaseUrl();
	url.AddPathComponent("config");
	url.SetParam("warehouse", catalog.warehouse);
	auto response = catalog.auth_handler->GetRequest(context, url);
	if (response->status != HTTPStatusCode::OK_200) {
		throw InvalidConfigurationException("Request to '%s' returned a non-200 status code (%s), with reason: %s",
		                                    url.GetURL(), EnumUtil::ToString(response->status), response->reason);
	}
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(response->body));
	auto *root = yyjson_doc_get_root(doc.get());
	return rest_api_objects::CatalogConfig::FromJSON(root);
}

} // namespace duckdb
