#include "catalog/rest/api/catalog_api.hpp"

#include "duckdb/main/secret/secret.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/json_document.hpp"

#include "catalog/rest/api/catalog_utils.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/iceberg_rest_backend.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "common/iceberg_utils.hpp"
#include "catalog/rest/api/api_utils.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "catalog/rest/storage/iceberg_table_secret_provider.hpp"
#include "catalog/rest/storage/authorization/sigv4_utils.hpp"

#include <sys/stat.h>

#include "rest_catalog/objects/list.hpp"
#include "rest_catalog/objects/iceberg_error_response.hpp"

namespace duckdb {

void CommitResult::Throw(const string &url) const {
	if (success) {
		return;
	}
	// Throw HTTPException so the status lands in ExtraInfo()["status_code"] for commit-state classification.
	if (error_) {
		auto error_copy = error_->Copy();
		error_copy._error.stack = vector<string>();
		throw HTTPException(
		    *this, "Request to '%s' returned a non-200 status code (%s). \n message: %s\n type: %s\n reason: %s\n", url,
		    EnumUtil::ToString(status), error_copy._error.message, error_copy._error.type, reason);
	}
	throw HTTPException(*this, "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s", url,
	                    EnumUtil::ToString(status), reason, body);
}

vector<string> IRCAPI::ParseSchemaName(const string &namespace_name) {
	return IcebergNamespace::Parse(namespace_name).levels;
}

[[noreturn]] static void ThrowException(const string &url, const HTTPResponse &response, const string &method) {
	D_ASSERT(!response.Success());

	if (response.HasRequestError()) {
		//! Request error - this means something went wrong performing the request
		throw IOException("%s request to endpoint '%s' failed: (ERROR %s)", method, url, response.GetRequestError());
	}
	unique_ptr<JSONDocument> error_doc;
	auto error_obj = ICUtils::GetErrorMessage(response.body, error_doc);
	rest_api_objects::IcebergErrorResponse error;
	if (error_obj.IsValid() && error.TryFromJSON(error_obj).empty()) {
		throw HTTPException(response, "%s request to endpoint '%s' returned an error response (HTTP %n). %s: %s",
		                    method, url, int(response.status), error._error.type, error._error.message);
	}
	if (!response.reason.empty()) {
		throw HTTPException(response, "%s request to endpoint '%s' returned an error response (HTTP %n). Reason: %s",
		                    method, url, int(response.status), response.reason);
	}

	//! If this was not a request error this means the server responded - report the response status and response
	throw HTTPException(response, "%s request to endpoint '%s' returned an error response (HTTP %n)", method, url,
	                    int(response.status));
}

static IRCEntryLookupStatus CheckVerificationResponse(ClientContext &context, HTTPStatusCode &status) {
	// Only a 404 establishes absence. Permission errors must not trigger IF EXISTS / IF NOT EXISTS behavior.
	switch (status) {
	case HTTPStatusCode::OK_200:
	case HTTPStatusCode::NoContent_204:
		return IRCEntryLookupStatus::EXISTS;
	case HTTPStatusCode::NotFound_404:
		return IRCEntryLookupStatus::NOT_FOUND;
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

bool IRCAPI::VerifyResponse(ClientContext &context, IcebergRestBackend &catalog, IRCEndpointBuilder &url_builder,
                            bool execute_head) {
	HTTPHeaders headers(*context.db);
	IRCEntryLookupStatus entry_status = IRCEntryLookupStatus::API_ERROR;
	unique_ptr<HTTPResponse> response;
	if (execute_head) {
		// First response of Head request
		response = catalog.auth_handler->Request(RequestType::HEAD_REQUEST, context, url_builder, headers);
		if (response->status == HTTPStatusCode::Forbidden_403 || response->status == HTTPStatusCode::Unauthorized_401) {
			// A denied existence check is authoritative, even if GET would return success or not found.
			ThrowException(url_builder.GetURLEncoded(), *response, "HEAD");
		}
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
		ThrowException(url_builder.GetURLEncoded(), *response, "GET");
	}
}

bool IRCAPI::VerifySchemaExistence(ClientContext &context, IcebergRestBackend &catalog,
                                   const vector<string> &namespace_items) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));
	bool execute_head =
	    catalog.supported_urls.find("HEAD /v1/{prefix}/namespaces/{namespace}") != catalog.supported_urls.end();
	return VerifyResponse(context, catalog, url_builder, execute_head);
}

bool IRCAPI::VerifyTableExistence(ClientContext &context, IcebergRestBackend &catalog,
                                  const vector<string> &namespace_items, const string &table) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("tables"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(table));
	bool execute_head = catalog.supported_urls.find("HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}") !=
	                    catalog.supported_urls.end();
	return VerifyResponse(context, catalog, url_builder, execute_head);
}

IcebergLoadTableRequest::IcebergLoadTableRequest(vector<string> namespace_items, string table_name,
                                                 optional_ptr<const LoadTableCachePublication> publication)
    : namespace_items(std::move(namespace_items)), table_name(std::move(table_name)) {
	if (publication) {
		cached_result = publication->cached_result;
		etag = publication->etag;
	}
}

IcebergLoadTableResult IcebergLoadTableRequest::Execute(ClientContext &context, IcebergRestBackend &catalog) const {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("tables"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(table_name));

	HTTPHeaders headers(*context.db);
	bool can_revalidate = cached_result && !etag.empty();
	if (catalog.options.access_mode == IRCAccessDelegationMode::VENDED_CREDENTIALS) {
		headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
		if (can_revalidate) {
			// ETags validate metadata, not credential lifetime. Storage without an Iceberg refresh
			// provider (e.g. Azure SAS) needs newly vended credentials from a full LoadTable response.
			auto &location = cached_result->result.metadata.location;
			can_revalidate = location && IcebergTableSecretProvider::SupportsStorageType(DetectStorageType(*location));
		}
	}
	if (can_revalidate) {
		headers.Insert("If-None-Match", etag);
	}
	auto result = catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers);
	IcebergLoadTableResult ret;
	ret.status_ = result->status;
	if (result->status == HTTPStatusCode::NotModified_304) {
		if (!can_revalidate) {
			throw HTTPException(*result, "LoadTable returned HTTP 304 without a cached response to revalidate");
		}
		ret.result_ = cached_result;
		ret.etag = result->headers.HasHeader("ETag") ? result->headers.GetHeaderValue("ETag") : etag;
		DUCKDB_LOG(
		    context, IcebergLogType, "Iceberg table '%s' is unchanged (HTTP 304); reusing cached metadata",
		    IcebergLogType::Redact(context, StringUtil::Format("%s.%s.%s", catalog.GetName().GetIdentifierName(),
		                                                       StringUtil::Join(namespace_items, "."), table_name)));
		return ret;
	}
	if (result->status != HTTPStatusCode::OK_200) {
		unique_ptr<JSONDocument> out_doc;
		auto error_obj = ICUtils::GetErrorMessage(result->body, out_doc);
		if (!error_obj.IsValid()) {
			throw InvalidConfigurationException(result->body);
		}
		ret.error_ = rest_api_objects::IcebergErrorResponse::FromJSON(error_obj);
		return ret;
	}
	auto doc = ICUtils::APIResultToDoc(result->body);
	auto metadata_root = doc->GetRoot();
	ret.result_ = make_shared_ptr<const IcebergLoadTableResponse>(ICUtils::ParseLoadTableResult(metadata_root));
	if (result->headers.HasHeader("ETag")) {
		ret.etag = result->headers.GetHeaderValue("ETag");
	}
	return ret;
}

static unique_ptr<HTTPResponse> LoadCredentials(ClientContext &context, IcebergRestBackend &catalog,
                                                const vector<string> &namespace_items, const string &table) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("tables"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(table));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("credentials"));

	HTTPHeaders headers(*context.db);
	if (catalog.options.access_mode == IRCAccessDelegationMode::VENDED_CREDENTIALS) {
		headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
	}
	return catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers);
}

IcebergLoadTableResult IRCAPI::GetTable(ClientContext &context, IcebergRestBackend &catalog,
                                        const vector<string> &namespace_items, const string &table_name,
                                        optional_ptr<const LoadTableCachePublication> publication) {
	return IcebergLoadTableRequest(namespace_items, table_name, publication).Execute(context, catalog);
}

APIResult<IRCAPICredentials> IRCAPI::GetTableCredentials(ClientContext &context, IcebergRestBackend &catalog,
                                                         const vector<string> &namespace_items,
                                                         const string &table_name) {
	auto ret = APIResult<IRCAPICredentials>();
	ret.status_ = HTTPStatusCode::OK_200;
	if (catalog.supported_urls.find("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials") ==
	    catalog.supported_urls.end()) {
		// Credential refresh needs a full response even if the table metadata has not changed.
		auto table_result = GetTable(context, catalog, namespace_items, table_name);
		if (table_result.error_) {
			ret.status_ = table_result.status_;
			ret.error_ = std::move(table_result.error_);
			return ret;
		}
		auto &credentials = ret.result_;
		if (table_result.result_->result.storage_credentials) {
			for (auto &credential : *table_result.result_->result.storage_credentials) {
				credentials.storage_credentials.push_back(credential.Copy());
			}
		}
		credentials.config.emplace();
		if (table_result.result_->result.config) {
			*credentials.config = *table_result.result_->result.config;
		}
		return ret;
	}
	auto result = LoadCredentials(context, catalog, namespace_items, table_name);
	if (result->status != HTTPStatusCode::OK_200) {
		unique_ptr<JSONDocument> out_doc;
		auto error_obj = ICUtils::GetErrorMessage(result->body, out_doc);
		if (!error_obj.IsValid()) {
			throw InvalidConfigurationException(result->body);
		}
		ret.status_ = result->status;
		ret.error_ = rest_api_objects::IcebergErrorResponse::FromJSON(error_obj);
		return ret;
	}
	auto doc = ICUtils::APIResultToDoc(result->body);
	auto metadata_root = doc->GetRoot();
	auto credentials = rest_api_objects::LoadCredentialsResponse::FromJSON(metadata_root);
	ret.result_.storage_credentials = std::move(credentials.storage_credentials);
	return ret;
}

APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>>
IRCAPI::GetNamespace(ClientContext &context, IcebergRestBackend &catalog, const vector<string> &namespace_items) {
	if (catalog.supported_urls.find("GET /v1/{prefix}/namespaces/{namespace}") == catalog.supported_urls.end()) {
		throw NotImplementedException("This Iceberg REST catalog server does not support this operation");
	}

	auto ret = APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>>();

	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));

	HTTPHeaders headers(*context.db);
	if (catalog.options.access_mode == IRCAccessDelegationMode::VENDED_CREDENTIALS) {
		headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
	}
	auto result = catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers);

	if (result->status != HTTPStatusCode::OK_200) {
		unique_ptr<JSONDocument> out_doc;
		auto error_obj = ICUtils::GetErrorMessage(result->body, out_doc);
		if (!error_obj.IsValid()) {
			throw InvalidConfigurationException(result->body);
		}
		ret.status_ = result->status;
		ret.error_ = rest_api_objects::IcebergErrorResponse::FromJSON(error_obj);
		return ret;
	}
	auto doc = ICUtils::APIResultToDoc(result->body);
	auto metadata_root = doc->GetRoot();
	ret.result_ = make_uniq<const rest_api_objects::GetNamespaceResponse>(
	    rest_api_objects::GetNamespaceResponse::FromJSON(metadata_root));
	return ret;
}

IcebergListTablesResult IRCAPI::GetTables(ClientContext &context, IcebergRestBackend &catalog,
                                          const vector<string> &namespace_items) {
	return IcebergListTablesRequest(namespace_items).Execute(context, catalog);
}

IcebergListTablesRequest::IcebergListTablesRequest(vector<string> namespace_items)
    : namespace_items(std::move(namespace_items)) {
}

//! The error a refused listing raises when the caller does not tolerate it
static ErrorData RefusedListingError(const string &url, const HTTPResponse &response) {
	try {
		ThrowException(url, response, "GET");
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
}

IcebergListTablesResult IcebergListTablesRequest::Execute(ClientContext &context, IcebergRestBackend &catalog) const {
	vector<rest_api_objects::TableIdentifier> all_identifiers;
	string page_token;

	do {
		auto url_builder = catalog.GetBaseUrl();
		url_builder.AddPrefixComponents(catalog.prefix);
		url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
		url_builder.AddPathComponent(
		    IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));
		url_builder.AddPathComponent(IRCPathComponent::RegularComponent("tables"));
		if (!page_token.empty()) {
			url_builder.SetParam("pageToken", IRCPathComponent::RegularComponent(page_token));
		}

		HTTPHeaders headers(*context.db);
		if (catalog.options.access_mode == IRCAccessDelegationMode::VENDED_CREDENTIALS) {
			headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
		}
		auto response = catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers);
		if (!response->Success()) {
			if (response->status == HTTPStatusCode::NotFound_404) {
				// A missing namespace has no entries. Native view binding may probe
				// the catalog's default namespace before trying replacement scans.
				return IcebergListTablesResult();
			}
			if (response->status == HTTPStatusCode::Forbidden_403 ||
			    response->status == HTTPStatusCode::Unauthorized_401) {
				// Catalog discovery can skip inaccessible schemas; explicit schema lookups must surface the error.
				// The caller decides, so report the refusal with the error it raises.
				IcebergListTablesResult refused;
				refused.refusal = RefusedListingError(url_builder.GetURLEncoded(), *response);
				return refused;
			}
			auto url = url_builder.GetURLEncoded();
			ThrowException(url, *response, "GET");
		}

		auto doc = ICUtils::APIResultToDoc(response->body);
		auto root = doc->GetRoot();
		auto list_tables_response = rest_api_objects::ListTablesResponse::FromJSON(root);

		if (!list_tables_response.identifiers) {
			throw NotImplementedException("List of 'identifiers' is missing, missing support for Iceberg V1");
		}
		auto &identifiers = *list_tables_response.identifiers;

		all_identifiers.insert(all_identifiers.end(), std::make_move_iterator(identifiers.begin()),
		                       std::make_move_iterator(identifiers.end()));

		if (list_tables_response.next_page_token) {
			page_token = list_tables_response.next_page_token->value;
		} else {
			page_token.clear();
		}
	} while (!page_token.empty());

	IcebergListTablesResult result;
	result.identifiers = std::move(all_identifiers);
	return result;
}

IcebergListSchemasResult IRCAPI::GetSchemas(ClientContext &context, IcebergRestBackend &catalog,
                                            const vector<string> &parent) {
	return IcebergListSchemasRequest(parent).Execute(context, catalog);
}

IcebergListSchemasRequest::IcebergListSchemasRequest(vector<string> parent) : parent(std::move(parent)) {
}

IcebergListSchemasResult IcebergListSchemasRequest::Execute(ClientContext &context, IcebergRestBackend &catalog) const {
	vector<IRCAPISchema> result;
	string page_token = "";
	do {
		auto url_builder = catalog.GetBaseUrl();
		url_builder.AddPrefixComponents(catalog.prefix);
		url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
		if (!parent.empty()) {
			url_builder.SetParam("parent", IRCPathComponent::NamespaceComponent(parent, catalog.namespace_separator));
		}
		if (!page_token.empty()) {
			url_builder.SetParam("pageToken", IRCPathComponent::RegularComponent(page_token));
		}
		HTTPHeaders headers(*context.db);
		auto response = catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers);
		if (!response->Success()) {
			if (response->status == HTTPStatusCode::Forbidden_403 ||
			    response->status == HTTPStatusCode::Unauthorized_401 ||
			    response->status == HTTPStatusCode::NotFound_404) {
				// when listing tables, if a user is not allowed to list a schema for one of the error reasons above
				// we log a warning to notify the user. We do not error, otherwise the user won't be able to see any
				// results.
				DUCKDB_LOG_WARNING(context, "GET %s returned %s", url_builder.GetURLEncoded(),
				                   EnumUtil::ToString(response->status));
				// return empty result if user cannot list schemas.
				return result;
			}
			auto url = url_builder.GetURLEncoded();
			ThrowException(url, *response, "GET");
		}

		auto doc = ICUtils::APIResultToDoc(response->body);
		auto root = doc->GetRoot();
		auto list_namespaces_response = rest_api_objects::ListNamespacesResponse::FromJSON(root);
		if (!list_namespaces_response.namespaces) {
			//! FIXME: old code expected 'namespaces' to always be present, but it's not a required property
			return result;
		}
		auto &schemas = *list_namespaces_response.namespaces;
		for (auto &schema : schemas) {
			IRCAPISchema schema_result;
			schema_result.catalog_name = catalog.GetName().GetIdentifierName();
			schema_result.items = std::move(schema.value);

			if (catalog.options.support_nested_namespaces) {
				auto new_parent = parent;
				new_parent.push_back(schema_result.items.back());
				auto nested_namespaces = IcebergListSchemasRequest(std::move(new_parent)).Execute(context, catalog);
				result.insert(result.end(), std::make_move_iterator(nested_namespaces.begin()),
				              std::make_move_iterator(nested_namespaces.end()));
			}
			result.push_back(schema_result);
		}

		if (list_namespaces_response.next_page_token) {
			page_token = list_namespaces_response.next_page_token->value;
		} else {
			page_token.clear();
		}
	} while (!page_token.empty());

	return result;
}

static CommitResult BuildCommitResult(ClientContext &context, const unique_ptr<HTTPResponse> &response) {
	CommitResult result;
	result.status = response->status;
	result.reason = response->reason;
	result.body = response->body;
	result.headers = response->headers;
	result.success = response->status == HTTPStatusCode::OK_200 || response->status == HTTPStatusCode::NoContent_204;
	if (result.success) {
		return result;
	}

	unique_ptr<JSONDocument> out_doc;
	auto error_obj = ICUtils::GetErrorMessage(response->body, out_doc);
	if (error_obj.IsValid()) {
		result.error_ = rest_api_objects::IcebergErrorResponse::FromJSON(error_obj);
		if (result.error_->_error.stack) {
			string stack_trace;
			for (const auto &str : *result.error_->_error.stack) {
				stack_trace.append(str + "\n");
			}
			DUCKDB_LOG(context, IcebergLogType, stack_trace);
		}
	}
	return result;
}

CommitResult IRCAPI::CommitMultiTableUpdate(ClientContext &context, IcebergRestBackend &catalog, const string &body) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("transactions"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("commit"));
	HTTPHeaders headers(*context.db);
	headers.Insert("Content-Type", "application/json");
	// Some catalogs (e.g. BigLake) require this header on the commit request itself, not just on
	// table/credential loads, or they reject the commit with a 400 asking for vended-credentials.
	if (catalog.options.access_mode == IRCAccessDelegationMode::VENDED_CREDENTIALS) {
		headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
	}
	ICUtils::LogPostBody(context, url_builder, body);
	auto response = catalog.auth_handler->Request(RequestType::POST_REQUEST, context, url_builder, headers, body);
	return BuildCommitResult(context, response);
}

CommitResult IRCAPI::CommitTableUpdate(ClientContext &context, IcebergRestBackend &catalog,
                                       const vector<string> &schema, const string &table, const string &body) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(schema, catalog.namespace_separator));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("tables"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(table));
	HTTPHeaders headers(*context.db);
	headers.Insert("Content-Type", "application/json");
	// Some catalogs (e.g. BigLake) require this header on the commit request itself, not just on
	// table/credential loads, or they reject the commit with a 400 asking for vended-credentials.
	if (catalog.options.access_mode == IRCAccessDelegationMode::VENDED_CREDENTIALS) {
		headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
	}
	ICUtils::LogPostBody(context, url_builder, body);
	auto response = catalog.auth_handler->Request(RequestType::POST_REQUEST, context, url_builder, headers, body);
	return BuildCommitResult(context, response);
}

void IRCAPI::CommitTableDelete(ClientContext &context, IcebergRestBackend &catalog, const vector<string> &schema,
                               const string &table) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(schema, catalog.namespace_separator));

	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("tables"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(table));
	url_builder.SetParam("purgeRequested", IRCPathComponent::RegularComponent(
	                                           Value::BOOLEAN(catalog.options.purge_requested).ToString()));

	HTTPHeaders headers(*context.db);
	auto response = catalog.auth_handler->Request(RequestType::DELETE_REQUEST, context, url_builder, headers);
	// Glue/S3Tables follow spec and return 204, apache/iceberg-rest-fixture docker image returns 200
	if (response->status != HTTPStatusCode::NoContent_204 && response->status != HTTPStatusCode::OK_200) {
		throw HTTPException(*response, "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s",
		                    url_builder.GetURLEncoded(), EnumUtil::ToString(response->status), response->reason,
		                    response->body);
	}
}

void IRCAPI::CommitTableRename(ClientContext &context, IcebergRestBackend &catalog, const string &body) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("tables"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("rename"));

	HTTPHeaders headers(*context.db);
	headers.Insert("Content-Type", "application/json");
	ICUtils::LogPostBody(context, url_builder, body);
	auto response = catalog.auth_handler->Request(RequestType::POST_REQUEST, context, url_builder, headers, body);
	// Glue/S3Tables follow spec and return 204, apache/iceberg-rest-fixture docker image returns 200
	if (response->status != HTTPStatusCode::NoContent_204 && response->status != HTTPStatusCode::OK_200) {
		throw HTTPException(*response, "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s",
		                    url_builder.GetURLEncoded(), EnumUtil::ToString(response->status), response->reason,
		                    response->body);
	}
}

bool IRCAPI::CommitNamespaceCreate(ClientContext &context, IcebergRestBackend &catalog, const string &schema,
                                   string body) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	HTTPHeaders headers(*context.db);
	headers.Insert("Content-Type", "application/json");
	ICUtils::LogPostBody(context, url_builder, body);
	auto response = catalog.auth_handler->Request(RequestType::POST_REQUEST, context, url_builder, headers, body);
	// some catalogs also answer other conflicts with a 409
	if (response->status == HTTPStatusCode::Conflict_409 &&
	    VerifySchemaExistence(context, catalog, ParseSchemaName(schema))) {
		return false;
	}
	if (response->status != HTTPStatusCode::OK_200) {
		throw HTTPException(*response, "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s",
		                    url_builder.GetURLEncoded(), EnumUtil::ToString(response->status), response->reason,
		                    response->body);
	}
	return true;
}

void IRCAPI::CommitNamespaceDrop(ClientContext &context, IcebergRestBackend &catalog,
                                 const vector<string> &namespace_items) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));

	HTTPHeaders headers(*context.db);
	string body = "";
	auto response = catalog.auth_handler->Request(RequestType::DELETE_REQUEST, context, url_builder, headers, body);
	// Glue/S3Tables follow spec and return 204, apache/iceberg-rest-fixture docker image returns 200
	if (response->status != HTTPStatusCode::NoContent_204 && response->status != HTTPStatusCode::OK_200) {
		throw HTTPException(*response, "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s",
		                    url_builder.GetURLEncoded(), EnumUtil::ToString(response->status), response->reason,
		                    response->body);
	}
}

void IRCAPI::CommitNamespacePropertiesUpdate(ClientContext &context, IcebergRestBackend &catalog, string body,
                                             const vector<string> &namespace_items) {
	if (catalog.supported_urls.find("POST /v1/{prefix}/namespaces/{namespace}/properties") ==
	    catalog.supported_urls.end()) {
		throw NotImplementedException("This Iceberg REST catalog server does not support this operation");
	}
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("properties"));

	HTTPHeaders headers(*context.db);
	headers.Insert("Content-Type", "application/json");
	auto response = catalog.auth_handler->Request(RequestType::POST_REQUEST, context, url_builder, headers, body);
	if (response->status != HTTPStatusCode::OK_200) {
		throw HTTPException(*response, "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s",
		                    url_builder.GetURLEncoded(), EnumUtil::ToString(response->status), response->reason,
		                    response->body);
	}
}

rest_api_objects::LoadTableResult IRCAPI::CommitNewTable(ClientContext &context, IcebergRestBackend &catalog,
                                                         const vector<string> &namespace_items,
                                                         const IcebergCreateTableRequest &request) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("tables"));

	const auto stage_create = catalog.options.stage_create_tables;
	auto create_table_json = request.CreateTableToJSON(stage_create);

	try {
		HTTPHeaders headers(*context.db);
		headers.Insert("Content-Type", "application/json");
		// if you are creating a table with stage create, you need vended credentials
		if (catalog.options.access_mode == IRCAccessDelegationMode::VENDED_CREDENTIALS) {
			headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
		}
		ICUtils::LogPostBody(context, url_builder, create_table_json);
		auto response =
		    catalog.auth_handler->Request(RequestType::POST_REQUEST, context, url_builder, headers, create_table_json);
		if (response->status != HTTPStatusCode::OK_200) {
			throw HTTPException(
			    *response, "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s",
			    url_builder.GetURLEncoded(), EnumUtil::ToString(response->status), response->reason, response->body);
		}
		auto doc = ICUtils::APIResultToDoc(response->body);
		auto root = doc->GetRoot();
		auto load_table_result = ICUtils::ParseLoadTableResult(root);
		return load_table_result;
	} catch (const NotImplementedException &) {
		throw;
	} catch (const HTTPException &) {
		// Non-200 already classified by HTTP status; rethrow so the status survives.
		throw;
	} catch (const std::exception &e) {
		throw InvalidConfigurationException("Request to '%s' failed: %s", url_builder.GetURLEncoded(), e.what());
	}
}

// ─── View operations ─────────────────────────────────────────────────────────

IcebergListViewsResult IRCAPI::GetViews(ClientContext &context, IcebergRestBackend &catalog,
                                        const vector<string> &namespace_items) {
	return IcebergListViewsRequest(namespace_items).Execute(context, catalog);
}

IcebergListViewsRequest::IcebergListViewsRequest(vector<string> namespace_items)
    : namespace_items(std::move(namespace_items)) {
}

IcebergListViewsResult IcebergListViewsRequest::Execute(ClientContext &context, IcebergRestBackend &catalog) const {
	vector<rest_api_objects::TableIdentifier> all_identifiers;
	string page_token;

	do {
		auto url_builder = catalog.GetBaseUrl();
		url_builder.AddPrefixComponents(catalog.prefix);
		url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
		url_builder.AddPathComponent(
		    IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));
		url_builder.AddPathComponent(IRCPathComponent::RegularComponent("views"));
		if (!page_token.empty()) {
			url_builder.SetParam("pageToken", IRCPathComponent::RegularComponent(page_token));
		}

		HTTPHeaders headers(*context.db);
		auto response = catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers);
		if (!response->Success()) {
			if (response->status == HTTPStatusCode::NotFound_404) {
				// A missing namespace has no entries. Native view binding may probe
				// the catalog's default namespace before trying replacement scans.
				return IcebergListViewsResult();
			}
			if (response->status == HTTPStatusCode::Forbidden_403 ||
			    response->status == HTTPStatusCode::Unauthorized_401) {
				IcebergListViewsResult refused;
				refused.refusal = RefusedListingError(url_builder.GetURLEncoded(), *response);
				return refused;
			}
			auto url = url_builder.GetURLEncoded();
			ThrowException(url, *response, "GET");
		}

		auto doc = ICUtils::APIResultToDoc(response->body);
		auto root = doc->GetRoot();
		auto list_response = rest_api_objects::ListTablesResponse::FromJSON(root);

		if (list_response.identifiers) {
			all_identifiers.insert(all_identifiers.end(), std::make_move_iterator(list_response.identifiers->begin()),
			                       std::make_move_iterator(list_response.identifiers->end()));
		}

		if (list_response.next_page_token) {
			page_token = list_response.next_page_token->value;
		} else {
			page_token.clear();
		}
	} while (!page_token.empty());

	IcebergListViewsResult result;
	result.identifiers = std::move(all_identifiers);
	return result;
}

IcebergLoadViewResult IRCAPI::GetView(ClientContext &context, IcebergRestBackend &catalog,
                                      const vector<string> &namespace_items, const string &view_name) {
	return IcebergLoadViewRequest(namespace_items, view_name).Execute(context, catalog);
}

IcebergLoadViewRequest::IcebergLoadViewRequest(vector<string> namespace_items, string view_name)
    : namespace_items(std::move(namespace_items)), view_name(std::move(view_name)) {
}

IcebergLoadViewResult IcebergLoadViewRequest::Execute(ClientContext &context, IcebergRestBackend &catalog) const {
	auto ret = IcebergLoadViewResult();

	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("views"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(view_name));

	HTTPHeaders headers(*context.db);
	auto response = catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers);
	ret.status_ = response->status;
	if (response->status != HTTPStatusCode::OK_200) {
		unique_ptr<JSONDocument> out_doc;
		auto error_obj = ICUtils::GetErrorMessage(response->body, out_doc);
		if (!error_obj.IsValid()) {
			throw InvalidConfigurationException(response->body);
		}
		ret.error_ = rest_api_objects::IcebergErrorResponse::FromJSON(error_obj);
		return ret;
	}
	auto doc = ICUtils::APIResultToDoc(response->body);
	auto root = doc->GetRoot();
	ret.result_ = make_uniq<rest_api_objects::LoadViewResult>(rest_api_objects::LoadViewResult::FromJSON(root));
	return ret;
}

void IRCAPI::CommitNewView(ClientContext &context, IcebergRestBackend &catalog, const vector<string> &namespace_items,
                           const string &json_body) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(namespace_items, catalog.namespace_separator));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("views"));

	HTTPHeaders headers(*context.db);
	headers.Insert("Content-Type", "application/json");
	auto response = catalog.auth_handler->Request(RequestType::POST_REQUEST, context, url_builder, headers, json_body);
	if (response->status != HTTPStatusCode::OK_200) {
		throw InvalidConfigurationException(
		    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s",
		    url_builder.GetURLEncoded(), EnumUtil::ToString(response->status), response->reason, response->body);
	}
}

void IRCAPI::CommitViewDelete(ClientContext &context, IcebergRestBackend &catalog, const vector<string> &schema,
                              const string &view_name) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(schema, catalog.namespace_separator));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("views"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(view_name));

	HTTPHeaders headers(*context.db);
	auto response = catalog.auth_handler->Request(RequestType::DELETE_REQUEST, context, url_builder, headers);
	if (response->status != HTTPStatusCode::NoContent_204 && response->status != HTTPStatusCode::OK_200) {
		throw InvalidConfigurationException(
		    "Request to '%s' returned a non-200 status code (%s), with reason: %s, body: %s",
		    url_builder.GetURLEncoded(), EnumUtil::ToString(response->status), response->reason, response->body);
	}
}

rest_api_objects::CatalogConfig IRCAPI::GetCatalogConfig(ClientContext &context, IcebergRestBackend &catalog,
                                                         const string &warehouse) {
	auto url_builder = catalog.GetBaseUrl();
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("config"));
	if (!warehouse.empty()) {
		url_builder.SetParam("warehouse", IRCPathComponent::RegularComponent(warehouse));
	}
	string body = "";
	HTTPHeaders headers(*context.db);
	auto response = catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers, body);
	if (response->status != HTTPStatusCode::OK_200) {
		// Attach-time config read (not a commit path), so this keeps InvalidConfigurationException.
		throw InvalidConfigurationException("Request to '%s' returned a non-200 status code (%s), with reason: %s",
		                                    url_builder.GetURLEncoded(), EnumUtil::ToString(response->status),
		                                    response->reason);
	}
	auto doc = ICUtils::APIResultToDoc(response->body);
	auto root = doc->GetRoot();
	return rest_api_objects::CatalogConfig::FromJSON(root);
}

} // namespace duckdb
