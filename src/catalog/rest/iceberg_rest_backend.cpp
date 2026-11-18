#include "catalog/rest/iceberg_rest_backend.hpp"

#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/logging/logger.hpp"

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "catalog/rest/iceberg_rest_commit.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "iceberg_logging.hpp"
#include "rest_catalog/objects/catalog_config.hpp"
#include "rest_catalog/objects/iceberg_error_response.hpp"
#include "rest_catalog/objects/storage_credential.hpp"

namespace duckdb {

IcebergRestBackend::IcebergRestBackend(const Identifier &catalog_name, IcebergAttachOptions options_p,
                                       unique_ptr<IcebergAuthorization> auth_handler_p)
    : catalog_name(catalog_name), auth_handler(std::move(auth_handler_p)), base_uri(options_p.catalog_uri),
      version("v1"), options(std::move(options_p)), table_request_cache(options), warehouse(options.warehouse) {
}

IcebergRestBackend::~IcebergRestBackend() {
}

IcebergRestBackend &IcebergRestBackend::Get(Catalog &catalog) {
	return catalog.Cast<IcebergCatalog>().GetBackend().Cast<IcebergRestBackend>();
}

//===--------------------------------------------------------------------===//
// Status mapping
//===--------------------------------------------------------------------===//

IcebergCatalogStatus IcebergRestBackend::MapHTTPStatus(HTTPStatusCode status) {
	switch (status) {
	case HTTPStatusCode::OK_200:
	case HTTPStatusCode::NoContent_204:
	case HTTPStatusCode::NotModified_304:
		return IcebergCatalogStatus::OK;
	case HTTPStatusCode::NotFound_404:
		return IcebergCatalogStatus::NOT_FOUND;
	case HTTPStatusCode::Unauthorized_401:
	case HTTPStatusCode::Forbidden_403:
		return IcebergCatalogStatus::FORBIDDEN;
	case HTTPStatusCode::Conflict_409:
		return IcebergCatalogStatus::CONFLICT;
	case HTTPStatusCode::NotImplemented_501:
	case HTTPStatusCode::NotAcceptable_406:
		return IcebergCatalogStatus::UNSUPPORTED;
	default:
		break;
	}
	auto code = static_cast<uint16_t>(status);
	if (code >= 400 && code < 500) {
		return IcebergCatalogStatus::FAILED;
	}
	//! A server error, or no response at all: the request may have taken effect
	return IcebergCatalogStatus::UNKNOWN_OUTCOME;
}

IcebergCatalogError IcebergRestBackend::MakeError(const string &operation, HTTPStatusCode status,
                                                  const rest_api_objects::IcebergErrorResponse &error) {
	auto message = StringUtil::Format("%s endpoint returned response code %s with message \"%s\"", operation,
	                                  EnumUtil::ToString(status), error._error.message);
	return IcebergCatalogError(MapHTTPStatus(status), ErrorData(ExceptionType::HTTP, message), error._error.type);
}

vector<IcebergStorageCredential>
IcebergRestBackend::ToStorageCredentials(const vector<rest_api_objects::StorageCredential> &credentials) {
	vector<IcebergStorageCredential> result;
	result.reserve(credentials.size());
	for (auto &credential : credentials) {
		IcebergStorageCredential converted;
		converted.prefix = credential.prefix;
		converted.config = credential.config;
		result.push_back(std::move(converted));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Configuration
//===--------------------------------------------------------------------===//

IRCEndpointBuilder IcebergRestBackend::GetBaseUrl() const {
	auto url_builder = IRCEndpointBuilder();
	url_builder.SetHost(base_uri);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(version));

	return url_builder;
}

void IcebergRestBackend::AddDefaultSupportedEndpoints() {
	// insert namespaces based on REST API spec.
	// List namespaces
	supported_urls.insert("GET /v1/{prefix}/namespaces");
	// create namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces");
	// Load metadata for a Namespace
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}");
	// Drop a namespace
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}");
	// set or remove properties on a namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/properties");
	// list all table identifiers
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables");
	// create table in the namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables");
	// get table from the catalog
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// commit updates to a tbale
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// drop table from a catalog
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// Register a table using given metadata file location.
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/register");
	// send metrics report to this endpoint to be processed by the backend
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics");
	// Rename a table from one identifier to another.
	supported_urls.insert("POST /v1/{prefix}/tables/rename");
	// commit updates to multiple tables in an atomic transaction
	supported_urls.insert("POST /v1/{prefix}/transactions/commit");
	// Views aren't in the spec defaults; catalogs that support them advertise via /v1/config
}

void IcebergRestBackend::AddS3TablesEndpoints() {
	// insert namespaces based on REST API spec.
	// List namespaces
	supported_urls.insert("GET /v1/{prefix}/namespaces");
	// create namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces");
	// Load metadata for a Namespace
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}");
	// Drop a namespace
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}");
	// list all table identifiers
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables");
	// create table in the namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables");
	// get table from the catalog
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// commit updates to a table
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// drop table from a catalog
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// Rename a table from one identifier to another.
	supported_urls.insert("POST /v1/{prefix}/tables/rename");
	// table exists
	supported_urls.insert("HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// namespace exists
	supported_urls.insert("HEAD /v1/{prefix}/namespaces/{namespace}");
}

void IcebergRestBackend::AddGlueEndpoints() {
	// insert namespaces based on REST API spec.
	// List namespaces
	supported_urls.insert("GET /v1/{prefix}/namespaces");
	// create namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces");
	// Load metadata for a Namespace
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}");
	// Drop a namespace
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}");
	// list all table identifiers
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables");
	// create table in the namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables");
	// get table from the catalog
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// table exists
	supported_urls.insert("HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// commit updates to a table
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// drop table from a catalog
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}");
}

void IcebergRestBackend::ParsePrefix() {
	prefix.clear();
	auto &prefix_property = options.prefix;
	if (prefix_property.empty()) {
		return;
	}

	auto decoded_prefix = StringUtil::URLDecode(prefix_property);
	if (options.encode_entire_prefix || !StringUtil::Equals(decoded_prefix, prefix_property)) {
		prefix.push_back(std::move(decoded_prefix));
	} else {
		prefix = StringUtil::Split(decoded_prefix, '/');
	}
}

void IcebergRestBackend::Configure(ClientContext &context, const rest_api_objects::CatalogConfig &catalog_config,
                                   IcebergEndpointType endpoint_type) {
	this->endpoint_type = endpoint_type;
	base_uri = options.catalog_uri;
	StringUtil::RTrim(base_uri, "/");
	if (base_uri.empty()) {
		throw InvalidConfigurationException("Missing 'uri' option for Iceberg attach");
	}
	options.catalog_uri = base_uri;
	warehouse = options.warehouse;
	ParsePrefix();
	namespace_separator = options.namespace_separator;

	catalog_properties = catalog_config.defaults;
	for (auto &entry : catalog_config.overrides) {
		catalog_properties[entry.first] = entry.second;
	}

	supported_urls.clear();
	if (auto &endpoints = catalog_config.endpoints) {
		for (auto &endpoint : *endpoints) {
			supported_urls.insert(endpoint);
		}
	}
	// should be if s3tables
	if (!catalog_config.endpoints && endpoint_type == IcebergEndpointType::AWS_S3TABLES) {
		supported_urls.clear();
		AddS3TablesEndpoints();
	} else if (!catalog_config.endpoints && endpoint_type == IcebergEndpointType::AWS_GLUE) {
		supported_urls.clear();
		AddGlueEndpoints();
	} else if (!catalog_config.endpoints) {
		AddDefaultSupportedEndpoints();
	}

	if (prefix.empty()) {
		DUCKDB_LOG(context, IcebergLogType, "No prefix found for catalog with warehouse value %s", warehouse);
	}

	capabilities.list_views = supported_urls.count("GET /v1/{prefix}/namespaces/{namespace}/views");
	capabilities.load_views = supported_urls.count("GET /v1/{prefix}/namespaces/{namespace}/views/{view}");
	capabilities.create_views = supported_urls.count("POST /v1/{prefix}/namespaces/{namespace}/views");
	//! Deleting a Glue namespace also deletes its tables
	capabilities.drop_namespace = endpoint_type == IcebergEndpointType::AWS_GLUE
	                                  ? IcebergDropNamespaceMode::CASCADE_ONLY
	                                  : IcebergDropNamespaceMode::RESTRICT;
	// some catalogs reject the multi-table transactions/commit endpoint; DISABLE_MULTI_TABLE_COMMIT opts out of it
	capabilities.atomic_multi_table_commit =
	    !options.disable_multi_table_commit && supported_urls.count("POST /v1/{prefix}/transactions/commit");
	//! A stage-created table is created by its first commit, a single-table request
	capabilities.atomic_commit_with_creates = false;
	//! Every namespace, view, drop and rename is its own request
	capabilities.atomic_catalog_commit = false;
	//! certain catalogs (S3 Tables) don't allow deletes and garbage-collect the files themselves
	capabilities.delete_written_files = options.remove_files_on_delete;
}

string IcebergRestBackend::ProductName() const {
	if (endpoint_type == IcebergEndpointType::AWS_GLUE) {
		return "AWS Glue";
	}
	return Name();
}

//===--------------------------------------------------------------------===//
// Namespaces
//===--------------------------------------------------------------------===//

bool IcebergRestBackend::NamespaceExists(ClientContext &context, const IcebergNamespace &ns) {
	return IRCAPI::VerifySchemaExistence(context, *this, ns.levels);
}

vector<IcebergNamespace> IcebergRestBackend::ListNamespaces(ClientContext &context, const IcebergNamespace &parent) {
	auto schemas = IcebergListSchemasRequest(parent.levels).Execute(context, *this);
	vector<IcebergNamespace> result;
	result.reserve(schemas.size());
	for (auto &schema : schemas) {
		result.emplace_back(std::move(schema.items));
	}
	return result;
}

IcebergCatalogResult<case_insensitive_map_t<string>>
IcebergRestBackend::LoadNamespaceProperties(ClientContext &context, const IcebergNamespace &ns) {
	auto result = IRCAPI::GetNamespace(context, *this, ns.levels);
	if (result.error_) {
		return MakeError("GetNamespace", result.status_, *result.error_);
	}
	case_insensitive_map_t<string> properties;
	if (auto &namespace_properties = result.result_->properties) {
		properties = *namespace_properties;
	}
	return properties;
}

//===--------------------------------------------------------------------===//
// Tables and views
//===--------------------------------------------------------------------===//

string IcebergRestBackend::GetEntryKey(const IcebergNamespace &ns, const string &name) const {
	if (ns.levels.empty()) {
		return name;
	}
	auto schema_component = IRCPathComponent::NamespaceComponent(ns.levels, namespace_separator);
	return schema_component.encoded + "." + name;
}

IcebergCatalogResult<vector<IcebergListedEntry>>
IcebergRestBackend::ListEntries(ClientContext &context, const IcebergNamespace &ns, IcebergEntryKind kind) {
	IcebergListTablesResult listing;
	if (kind == IcebergEntryKind::TABLE) {
		listing = IcebergListTablesRequest(ns.levels).Execute(context, *this);
	} else {
		listing = IcebergListViewsRequest(ns.levels).Execute(context, *this);
	}
	if (listing.refusal) {
		return IcebergCatalogError(IcebergCatalogStatus::FORBIDDEN, std::move(*listing.refusal));
	}
	vector<IcebergListedEntry> entries;
	entries.reserve(listing.identifiers.size());
	for (auto &identifier : listing.identifiers) {
		IcebergListedEntry entry;
		entry.name = std::move(identifier.name);
		entry.kind = kind;
		entries.push_back(std::move(entry));
	}
	return entries;
}

shared_ptr<const IcebergLoadedTable> IcebergRestBackend::GetCachedTable(ClientContext &context,
                                                                        const string &table_key) {
	// Only check cache if MAX_TABLE_STALENESS option is set
	if (!options.max_table_staleness_micros.IsValid()) {
		return nullptr;
	}
	shared_ptr<const IcebergLoadedTable> result;
	table_request_cache.Get(context, table_key,
	                        [&](shared_ptr<const IcebergLoadTableResponse> cached) { result = std::move(cached); });
	return result;
}

namespace {

//! A LoadTable request: it revalidates the response the cache held when the load began, with its ETag, and offers
//! the result to the cache when it is published
class IcebergRestTableLoad : public IcebergTableLoad {
public:
	IcebergRestTableLoad(IcebergRestBackend &backend, IcebergEntryId id_p,
	                     unique_ptr<LoadTableCachePublication> publication_p)
	    : IcebergTableLoad(std::move(id_p)), backend(backend), publication(std::move(publication_p)),
	      request(id.ns.levels, id.name, publication.get()) {
	}

	IcebergCatalogResult<shared_ptr<const IcebergLoadedTable>> Execute(ClientContext &context) override {
		auto result = request.Execute(context, backend);
		if (result.error_) {
			auto error = IcebergRestBackend::MakeError("GetTableInformation", result.status_, *result.error_);
			// Glue returns 404 when a table is not an Iceberg Table with the error message
			// "input table is not an iceberg table" of type "NoSuchIcebergTableException"
			// see test/sql/cloud/test_glue_catalog_with_other_tables.test for testing
			if (error.status == IcebergCatalogStatus::NOT_FOUND &&
			    result.error_->_error.type == "NoSuchIcebergTableException") {
				error.status = IcebergCatalogStatus::NOT_ICEBERG;
			}
			return error;
		}
		etag = std::move(result.etag);
		return shared_ptr<const IcebergLoadedTable>(std::move(result.result_));
	}

	bool Publish(shared_ptr<const IcebergLoadedTable> table) override {
		auto response = shared_ptr_cast<const IcebergLoadedTable, const IcebergLoadTableResponse>(std::move(table));
		return publication->TryPublish(std::move(response), std::move(etag));
	}

private:
	IcebergRestBackend &backend;
	unique_ptr<LoadTableCachePublication> publication;
	IcebergLoadTableRequest request;
	//! The ETag of the executed request
	string etag;
};

} // namespace

unique_ptr<IcebergTableLoad> IcebergRestBackend::BeginLoadTable(ClientContext &context, IcebergEntryId id,
                                                                const string &table_key) {
	return make_uniq<IcebergRestTableLoad>(*this, std::move(id), table_request_cache.BeginLoad(table_key));
}

void IcebergRestBackend::OnTableCreated(const string &table_key, shared_ptr<const IcebergLoadedTable> table) {
	table_request_cache.SetOrOverwrite(
	    table_key, shared_ptr_cast<const IcebergLoadedTable, const IcebergLoadTableResponse>(std::move(table)));
}

void IcebergRestBackend::OnTableChanged(const string &table_key, optional_ptr<const IcebergLoadedTable> table,
                                        bool dropped) {
	// Without MAX_TABLE_STALENESS a changed table is revalidated on its next load anyway
	if (!dropped && !options.max_table_staleness_micros.IsValid()) {
		return;
	}
	table_request_cache.EvictIfCurrent(table_key, table);
}

IcebergCatalogResult<shared_ptr<const IcebergLoadedView>> IcebergRestBackend::LoadView(ClientContext &context,
                                                                                       const IcebergEntryId &id) {
	auto result = IcebergLoadViewRequest(id.ns.levels, id.name).Execute(context, *this);
	if (result.error_) {
		return MakeError("GetView", result.status_, *result.error_);
	}
	auto view = make_shared_ptr<IcebergLoadedView>();
	view->metadata = std::move(result.result_->metadata);
	return shared_ptr<const IcebergLoadedView>(std::move(view));
}

//===--------------------------------------------------------------------===//
// Statement time and commit
//===--------------------------------------------------------------------===//

IcebergCreatedTable IcebergRestBackend::CreateTable(ClientContext &context, const IcebergEntryId &id,
                                                    IcebergNewTable &table) {
	// some catalogs (e.g. AWS Glue) do not assign a table location server-side; derive one from the namespace's
	// 'location' property
	if (table.location.empty() && options.default_table_location_from_namespace) {
		auto &properties = table.namespace_properties();
		auto ns_location_it = properties.find("location");
		if (ns_location_it != properties.end() && !ns_location_it->second.empty()) {
			auto location = ns_location_it->second;
			StringUtil::RTrim(location, "/");
			table.location = location + "/" + table.name;
		}
	}
	IcebergCreateTableRequest request(table.name, table.schema, std::move(table.partition_spec),
	                                  std::move(table.sort_order), table.format_version, table.properties,
	                                  table.location);

	// Immediately create the table with stage_create = true to get metadata & data location(s)
	// transaction commit will either commit with data (OR) create the table with stage_create = false
	IcebergCreatedTable created;
	created.initial =
	    make_shared_ptr<const IcebergLoadTableResponse>(IRCAPI::CommitNewTable(context, *this, id.ns.levels, request));
	const bool stage_create = options.stage_create_tables;
	//! Without stage-create, the table exists in the catalog already
	created.exists_in_catalog = !stage_create;
	//! if we stage created the table, we add an assert create
	created.needs_assert_create = stage_create;
	// some catalogs fully initialize metadata during non-staged CREATE TABLE and reject follow-up metadata updates
	created.add_create_updates = stage_create || !options.skip_create_table_metadata_updates;
	return created;
}

void IcebergRestBackend::Commit(ClientContext &context, IcebergCommitPlan &plan) {
	try {
		IcebergRestCommit(*this, context, plan).Run();
	} catch (std::exception &ex) {
		if (IcebergRestCommit::CommitOutcomeUnknown(ErrorData(ex))) {
			// Commit may have landed (CommitStateUnknownException); keep the written files and created tables.
			plan.MarkOutcomeUnknown();
		}
		throw;
	}
}

void IcebergRestBackend::Abort(ClientContext &context, IcebergCommitPlan &plan) {
	IcebergRestCommit::DropCreatedTables(*this, context, plan);
}

} // namespace duckdb
