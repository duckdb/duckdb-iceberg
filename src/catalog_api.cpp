#include "catalog_api.hpp"
#include "include/catalog_api.hpp"

#include "catalog_utils.hpp"
#include "iceberg_logging.hpp"
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
#include "include/storage/irc_authorization.hpp"
#include "include/storage/irc_catalog.hpp"

#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;
namespace duckdb {

vector<string> IRCAPI::ParseSchemaName(const string &namespace_name) {
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
	D_ASSERT(!items.empty());
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

static IRCEntryLookupStatus CheckVerificationResponse(ClientContext &context, HTTPStatusCode &status) {
	// The following response codes return "schema does not exist"
	// This list can change, some error codes we want to surface to the user (i.e PaymentRequired_402)
	// but others not (Forbidden_403).
	// We log 400, 401, and 500 just in case.
	switch (status) {
	case HTTPStatusCode::OK_200:
	case HTTPStatusCode::NoContent_204:
		return IRCEntryLookupStatus::EXISTS;
	case HTTPStatusCode::Forbidden_403:
	case HTTPStatusCode::NotFound_404:
		return IRCEntryLookupStatus::NOT_FOUND;
		break;
	case HTTPStatusCode::BadRequest_400:
	case HTTPStatusCode::Unauthorized_401:
#ifndef DEBUG
		// Our local docker IRC can return 500 randomly, in debug we want to throw the error
		// Glue returns 500 if the schema doesn't exist.
	case HTTPStatusCode::InternalServerError_500:
#endif
		DUCKDB_LOG(context, IcebergLogType, "VerifySchemaExistence returned status code %s",
		           EnumUtil::ToString(status));
		return IRCEntryLookupStatus::API_ERROR;
	default:
		break;
	}
	return IRCEntryLookupStatus::API_ERROR;
}

bool IRCAPI::VerifyResponse(ClientContext &context, IRCatalog &catalog, IRCEndpointBuilder &url_builder,
                            bool execute_head) {
	HTTPHeaders headers(*context.db);
	IRCEntryLookupStatus entry_status = IRCEntryLookupStatus::API_ERROR;
	unique_ptr<HTTPResponse> response;
	if (execute_head) {
		// First response of Head request
		response = catalog.auth_handler->Request(RequestType::HEAD_REQUEST, context, url_builder, headers);
		// the httputil currently only sets 200 and 304 response to success
		// for AWS all responses < 400 are successful
		entry_status = CheckVerificationResponse(context, response->status);
		switch (entry_status) {
		case IRCEntryLookupStatus::EXISTS:
			return true;
		case IRCEntryLookupStatus::NOT_FOUND:
			return false;
		default:
			break;
		}
	}
	D_ASSERT(entry_status == IRCEntryLookupStatus::API_ERROR);
	response = catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers);
	// if execute head has a weird response, fall back to GET just in case.
	// check response of GET REQUEST
	entry_status = CheckVerificationResponse(context, response->status);
	switch (entry_status) {
	case IRCEntryLookupStatus::EXISTS:
		return true;
	case IRCEntryLookupStatus::NOT_FOUND:
		return false;
	default:
		// both head and get responses have returned a status that is an
		// error status
		ThrowException(url_builder.GetURL(), *response, response->reason);
	}
}

bool IRCAPI::VerifySchemaExistence(ClientContext &context, IRCatalog &catalog, const string &schema) {
	auto namespace_items = ParseSchemaName(schema);
	auto schema_name = GetEncodedSchemaName(namespace_items);

	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");
	url_builder.AddPathComponent(schema_name);
	bool execute_head =
	    catalog.supported_urls.find("HEAD /v1/{prefix}/namespaces/{namespace}") != catalog.supported_urls.end();
	return VerifyResponse(context, catalog, url_builder, execute_head);
}

bool IRCAPI::VerifyTableExistence(ClientContext &context, IRCatalog &catalog, const IRCSchemaEntry &schema,
                                  const string &table) {
	auto schema_name = GetEncodedSchemaName(schema.namespace_items);

	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");
	url_builder.AddPathComponent(schema_name);
	url_builder.AddPathComponent("tables");
	url_builder.AddPathComponent(table);
	bool execute_head = catalog.supported_urls.find("HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}") !=
	                    catalog.supported_urls.end();
	return VerifyResponse(context, catalog, url_builder, execute_head);
}

static unique_ptr<HTTPResponse> GetTableMetadata(ClientContext &context, IRCatalog &catalog,
                                                 const IRCSchemaEntry &schema, const string &table) {
	auto schema_name = IRCAPI::GetEncodedSchemaName(schema.namespace_items);

	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("namespaces");
	url_builder.AddPathComponent(schema_name);
	url_builder.AddPathComponent("tables");
	url_builder.AddPathComponent(table);

	HTTPHeaders headers(*context.db);
	if (catalog.attach_options.access_mode == IRCAccessDelegationMode::VENDED_CREDENTIALS) {
		headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
	}
	return catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers);
}

APIResult<rest_api_objects::LoadTableResult> IRCAPI::GetTable(ClientContext &context, IRCatalog &catalog,
                                                              const IRCSchemaEntry &schema, const string &table_name) {
	auto ret = APIResult<rest_api_objects::LoadTableResult>();
	auto result = GetTableMetadata(context, catalog, schema, table_name);
	if (result->status != HTTPStatusCode::OK_200) {
		yyjson_val *error_obj = ICUtils::get_error_message(result->body);
		if (error_obj == nullptr) {
			throw InvalidConfigurationException(result->body);
		}
		ret.has_error = true;
		ret.status_ = result->status;
		ret.error_ = rest_api_objects::IcebergErrorResponse::FromJSON(error_obj);
		return ret;
	}
	ret.has_error = false;
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(result->body));
	auto *metadata_root = yyjson_doc_get_root(doc.get());
	ret.result_ = rest_api_objects::LoadTableResult::FromJSON(metadata_root);
	return ret;
}

vector<rest_api_objects::TableIdentifier> IRCAPI::GetTables(ClientContext &context, IRCatalog &catalog,
                                                            const IRCSchemaEntry &schema) {
	auto schema_name = GetEncodedSchemaName(schema.namespace_items);
	vector<rest_api_objects::TableIdentifier> all_identifiers;
	string page_token;

	do {
		auto url_builder = catalog.GetBaseUrl();
		url_builder.AddPathComponent(catalog.prefix);
		url_builder.AddPathComponent("namespaces");
		url_builder.AddPathComponent(schema_name);
		url_builder.AddPathComponent("tables");
		if (!page_token.empty()) {
			url_builder.SetParam("pageToken", page_token);
		}

		HTTPHeaders headers(*context.db);
		if (catalog.attach_options.access_mode == IRCAccessDelegationMode::VENDED_CREDENTIALS) {
			headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
		}
		auto response = catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers);
		if (!response->Success()) {
			if (response->status == HTTPStatusCode::Forbidden_403 ||
			    response->status == HTTPStatusCode::Unauthorized_401) {
				// return empty result if user cannot list tables for a schema.
				vector<rest_api_objects::TableIdentifier> ret;
				return ret;
			}
			auto url = url_builder.GetURL();
			ThrowException(url, *response, "GET");
		}

		std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(response->body));
		auto *root = yyjson_doc_get_root(doc.get());
		auto list_tables_response = rest_api_objects::ListTablesResponse::FromJSON(root);

		if (!list_tables_response.has_identifiers) {
			throw NotImplementedException("List of 'identifiers' is missing, missing support for Iceberg V1");
		}

		all_identifiers.insert(all_identifiers.end(), std::make_move_iterator(list_tables_response.identifiers.begin()),
		                       std::make_move_iterator(list_tables_response.identifiers.end()));

		if (list_tables_response.has_next_page_token) {
			page_token = list_tables_response.next_page_token.value;
		} else {
			page_token.clear();
		}
	} while (!page_token.empty());

	return all_identifiers;
}

vector<IRCAPISchema> IRCAPI::GetSchemas(ClientContext &context, IRCatalog &catalog, const vector<string> &parent) {
	vector<IRCAPISchema> result;
	string page_token = "";
	do {
		auto endpoint_builder = catalog.GetBaseUrl();
		endpoint_builder.AddPathComponent(catalog.prefix);
		endpoint_builder.AddPathComponent("namespaces");
		if (!parent.empty()) {
			auto parent_name = GetSchemaName(parent);
			endpoint_builder.SetParam("parent", parent_name);
		}
		if (!page_token.empty()) {
			endpoint_builder.SetParam("pageToken", page_token);
		}
		HTTPHeaders headers(*context.db);
		auto response = catalog.auth_handler->Request(RequestType::GET_REQUEST, context, endpoint_builder, headers);
		if (!response->Success()) {
			if (response->status == HTTPStatusCode::Forbidden_403 ||
			    response->status == HTTPStatusCode::Unauthorized_401) {
				// return empty result if user cannot list schemas.
				return result;
			}
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

		if (list_namespaces_response.has_next_page_token) {
			page_token = list_namespaces_response.next_page_token.value;
		} else {
			page_token.clear();
		}
	} while (!page_token.empty());

	return result;
}

void IRCAPI::CommitMultiTableUpdate(ClientContext &context, IRCatalog &catalog, const string &body) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(catalog.prefix);
	url_builder.AddPathComponent("transactions");
	url_builder.AddPathComponent("commit");
	HTTPHeaders headers(*context.db);
	headers.Insert("Content-Type", "application/json");
	auto response = catalog.auth_handler->Request(RequestType::POST_REQUEST, context, url_builder, headers, body);
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
	HTTPHeaders headers(*context.db);
	headers.Insert("Content-Type", "application/json");
	auto response = catalog.auth_handler->Request(RequestType::POST_REQUEST, context, url_builder, headers, body);
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

	HTTPHeaders headers(*context.db);
	auto response = catalog.auth_handler->Request(RequestType::DELETE_REQUEST, context, url_builder, headers);
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
	HTTPHeaders headers(*context.db);
	headers.Insert("Content-Type", "application/json");
	auto response = catalog.auth_handler->Request(RequestType::POST_REQUEST, context, url_builder, headers, body);
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

	HTTPHeaders headers(*context.db);
	string body = "";
	auto response = catalog.auth_handler->Request(RequestType::DELETE_REQUEST, context, url_builder, headers, body);
	// Glue/S3Tables follow spec and return 204, apache/iceberg-rest-fixture docker image returns 200
	if (response->status != HTTPStatusCode::NoContent_204 && response->status != HTTPStatusCode::OK_200) {
		throw InvalidConfigurationException(
		    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s", url_builder.GetURL(),
		    EnumUtil::ToString(response->status), response->reason, response->body);
	}
}

rest_api_objects::LoadTableResult IRCAPI::CommitNewTable(ClientContext &context, IRCatalog &catalog,
                                                         const ICTableEntry *table) {
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
		HTTPHeaders headers(*context.db);
		headers.Insert("Content-Type", "application/json");
		// if you are creating a table with stage create, you need vended credentials
		if (catalog.attach_options.access_mode == IRCAccessDelegationMode::VENDED_CREDENTIALS) {
			headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
		}
		auto response =
		    catalog.auth_handler->Request(RequestType::POST_REQUEST, context, url_builder, headers, create_table_json);
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
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent("config");
	url_builder.SetParam("warehouse", catalog.warehouse);
	string body = "";
	HTTPHeaders headers(*context.db);
	auto response = catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers, body);
	if (response->status != HTTPStatusCode::OK_200) {
		throw InvalidConfigurationException("Request to '%s' returned a non-200 status code (%s), with reason: %s",
		                                    url_builder.GetURL(), EnumUtil::ToString(response->status),
		                                    response->reason);
	}
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(response->body));
	auto *root = yyjson_doc_get_root(doc.get());
	return rest_api_objects::CatalogConfig::FromJSON(root);
}

} // namespace duckdb
