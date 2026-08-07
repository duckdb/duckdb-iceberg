#include "catalog/rest/iceberg_catalog.hpp"

#include "duckdb/storage/database_size.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "common/iceberg_utils.hpp"
#include "iceberg_logging.hpp"
#include "catalog/rest/api/api_utils.hpp"
#include "rest_catalog/objects/catalog_config.hpp"

namespace duckdb {

void LoadTableResultCache::EvictIfCurrent(const IcebergTable &table) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto it = tables.find(table.GetTableKey());
	if (it == tables.end()) {
		return;
	}
	if (it->second.load_table_result.get() != table.initialization_source.get()) {
		return;
	}
	tables.erase(it);
}

IcebergCatalog::IcebergCatalog(AttachedDatabase &db_p, AccessMode access_mode,
                               unique_ptr<IcebergAuthorization> auth_handler, IcebergAttachOptions &attach_options_p,
                               const Identifier &default_schema)
    : Catalog(db_p), access_mode(access_mode), auth_handler(std::move(auth_handler)), uri(attach_options_p.endpoint),
      version("v1"), attach_options(attach_options_p), default_schema(default_schema),
      warehouse(attach_options.warehouse), schemas(*this), table_request_cache(attach_options) {
}

IcebergCatalog::~IcebergCatalog() = default;

//===--------------------------------------------------------------------===//
// Catalog API
//===--------------------------------------------------------------------===//

void IcebergCatalog::Initialize(bool load_builtin) {
}

void IcebergCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<IcebergSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> IcebergCatalog::LookupSchema(CatalogTransaction transaction,
                                                              const EntryLookupInfo &schema_lookup,
                                                              OnEntryNotFound if_not_found) {
	if (schema_lookup.GetEntryName() == DEFAULT_SCHEMA && default_schema != DEFAULT_SCHEMA) {
		// throws error if default schema is empty
		if (default_schema.empty() && if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		return GetSchema(transaction, default_schema, if_not_found);
	}

	auto &schema_name = schema_lookup.GetEntryName();
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name, if_not_found);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw CatalogException(schema_lookup.GetErrorContext(), "Schema with name \"%s\" not found", schema_name);
	}

	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

optional_ptr<CatalogEntry> IcebergCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	optional_ptr<ClientContext> context = transaction.GetContext();
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException(
		    "CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements");
	}

	D_ASSERT(context);
	auto &iceberg_transaction = IcebergTransaction::Get(*context, *this);
	auto &schema_name = info.GetQualifiedName().Schema().GetIdentifierName();
	auto created_schema = iceberg_transaction.created_schemas.find(schema_name);
	if (created_schema != iceberg_transaction.created_schemas.end()) {
		if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return created_schema->second.get();
		}
		throw CatalogException("Schema with name \"%s\" already exists", info.GetQualifiedName().Schema());
	}

	// Verify schema existence on the server first
	bool schema_exists = IRCAPI::VerifySchemaExistence(*context, *this, schema_name);

	if (schema_exists) {
		if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			// Schema already exists on the server - get or create a local entry and return it
			auto entry = schemas.GetEntry(*context, schema_name, OnEntryNotFound::RETURN_NULL);
			if (entry) {
				return entry;
			}
			auto new_schema = make_shared_ptr<IcebergSchemaEntry>(*this, info);
			schemas.AddEntry(schema_name, new_schema);
			iceberg_transaction.schemas[schema_name] = new_schema;
			return new_schema.get();
		}
		throw CatalogException("Schema with name \"%s\" already exists", info.GetQualifiedName().Schema());
	}

	// Schema does not exist - stage it locally and defer the server creation and catalog publication to commit
	auto new_schema = make_shared_ptr<IcebergSchemaEntry>(*this, info);
	auto result = new_schema.get();
	iceberg_transaction.created_schemas.emplace(schema_name, std::move(new_schema));
	return result;
}

void IcebergCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	if (info.cascade) {
		throw NotImplementedException(
		    "DROP SCHEMA <schema_name> CASCADE is not supported for Iceberg schemas currently");
	}

	// Verify schema existence on the server first
	bool schema_exists =
	    IRCAPI::VerifySchemaExistence(context, *this, info.GetQualifiedName().Name().GetIdentifierName());

	if (!schema_exists) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			// remove the entry if it exists locally
			// it could have been created during the bind phase.
			GetSchemas().RemoveEntry(info.GetQualifiedName().Name().GetIdentifierName());
			return;
		}
		throw CatalogException("Schema with name \"%s\" does not exist", info.GetQualifiedName().Name());
	}

	// Schema exists - defer the server deletion to commit
	auto &iceberg_transaction = IcebergTransaction::Get(context, *this);
	iceberg_transaction.deleted_schemas.insert(info.GetQualifiedName().Name().GetIdentifierName());
}

unique_ptr<LogicalOperator> IcebergCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                            TableCatalogEntry &table,
                                                            unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("IcebergCatalog BindCreateIndex");
}

bool IcebergCatalog::InMemory() {
	return false;
}

string IcebergCatalog::GetDBPath() {
	return warehouse;
}

DatabaseSize IcebergCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	return size;
}

ErrorData IcebergCatalog::SupportsCreateTable(BoundCreateTableInfo &info) {
	auto &base = info.Base().Cast<CreateTableInfo>();
	if (!base.sort_keys.empty()) {
		return ErrorData(ExceptionType::CATALOG,
		                 StringUtil::Format("SORTED BY is not supported for tables in a %s catalog", GetCatalogType()));
	}
	return ErrorData();
}

//===--------------------------------------------------------------------===//
// Iceberg REST Catalog
//===--------------------------------------------------------------------===//

IRCEndpointBuilder IcebergCatalog::GetBaseUrl() const {
	auto url_builder = IRCEndpointBuilder();
	url_builder.SetHost(uri);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(version));

	return url_builder;
}

unique_ptr<SecretEntry> IcebergCatalog::GetStorageSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	case_insensitive_set_t accepted_secret_types {"s3", "aws"};

	if (!secret_name.empty()) {
		auto secret_entry = context.db->GetSecretManager().GetSecretByName(transaction, secret_name);
		if (secret_entry) {
			auto secret_type = secret_entry->secret->GetType();
			if (accepted_secret_types.count(secret_type.GetIdentifierName())) {
				return secret_entry;
			}
			throw InvalidConfigurationException(
			    "Found a secret by the name of '%s', but it is not of an accepted type for a 'secret', "
			    "accepted types are: 's3' or 'aws', found '%s'",
			    secret_name, secret_type);
		}
		throw InvalidConfigurationException(
		    "No secret by the name of '%s' could be found, consider changing the 'secret'", secret_name);
	}

	for (auto &type : accepted_secret_types) {
		if (secret_name.empty()) {
			//! Lookup the default secret for this type
			auto secret_entry =
			    context.db->GetSecretManager().GetSecretByName(transaction, StringUtil::Format("__default_%s", type));
			if (secret_entry) {
				return secret_entry;
			}
		}
		auto secret_match = context.db->GetSecretManager().LookupSecret(transaction, type + "://", type);
		if (secret_match.HasMatch()) {
			return std::move(secret_match.secret_entry);
		}
	}
	throw InvalidConfigurationException("Could not find a valid storage secret (s3 or aws)");
}

IcebergSchemaSet &IcebergCatalog::GetSchemas() {
	return schemas;
}

unique_ptr<SecretEntry> IcebergCatalog::GetIcebergSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	unique_ptr<SecretEntry> secret_entry = nullptr;
	if (secret_name.empty()) {
		//! Try to find any secret with type 'iceberg'
		auto secret_match = context.db->GetSecretManager().LookupSecret(transaction, "", "iceberg");
		if (!secret_match.HasMatch()) {
			return nullptr;
		}
		secret_entry = std::move(secret_match.secret_entry);
	} else {
		secret_entry = context.db->GetSecretManager().GetSecretByName(transaction, secret_name);
	}
	return secret_entry;
}

unique_ptr<SecretEntry> IcebergCatalog::GetHTTPSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	unique_ptr<SecretEntry> secret_entry = nullptr;

	if (!secret_name.empty()) {
		secret_entry = context.db->GetSecretManager().GetSecretByName(transaction, secret_name);
		if (!secret_entry) {
			throw InternalException("Secret '%s' not found", secret_name);
		}
		auto http_kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		bool has_proxy = !http_kv_secret.TryGetValue("http_proxy").IsNull();
		if (has_proxy) {
			return secret_entry;
		}
	}
	auto secret_match = context.db->GetSecretManager().LookupSecret(transaction, "", "http");
	if (!secret_match.HasMatch()) {
		return nullptr;
	}
	secret_entry = std::move(secret_match.secret_entry);
	return secret_entry;
}
void IcebergCatalog::AddDefaultSupportedEndpoints() {
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
}

void IcebergCatalog::AddS3TablesEndpoints() {
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

void IcebergCatalog::AddGlueEndpoints() {
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

void IcebergCatalog::ParsePrefix() {
	// save overrides and defaults.
	// See https://iceberg.apache.org/docs/latest/configuration/#catalog-properties for sometimes used catalog
	// properties
	auto default_prefix_it = defaults.find("prefix");
	auto override_prefix_it = overrides.find("prefix");

	const string *prefix_property = nullptr;
	if (default_prefix_it != defaults.end()) {
		prefix_property = &default_prefix_it->second;
	}
	// Sometimes the prefix is in the overrides. Prefer the override prefix.
	if (override_prefix_it != overrides.end()) {
		prefix_property = &override_prefix_it->second;
	}
	if (!prefix_property) {
		return;
	}

	auto decoded_prefix = StringUtil::URLDecode(*prefix_property);
	if (attach_options.encode_entire_prefix || !StringUtil::Equals(decoded_prefix, *prefix_property)) {
		prefix.push_back(std::move(decoded_prefix));
	} else {
		prefix = StringUtil::Split(decoded_prefix, '/');
	}
}

void IcebergCatalog::ParseNamespaceSeparator() {
	auto default_namespace_separator_it = defaults.find("namespace-separator");
	auto override_namespace_separator_it = overrides.find("namespace-separator");

	const string *namespace_separator_property = nullptr;
	if (default_namespace_separator_it != defaults.end()) {
		namespace_separator_property = &default_namespace_separator_it->second;
	}
	// Sometimes the namespace_separator is in the overrides. Prefer the override namespace_separator.
	if (override_namespace_separator_it != overrides.end()) {
		namespace_separator_property = &override_namespace_separator_it->second;
	}
	if (!namespace_separator_property) {
		return;
	}
	namespace_separator = *namespace_separator_property;
}

void IcebergCatalog::GetConfig(ClientContext &context, IcebergEndpointType &endpoint_type) {
	// set the prefix to be empty. To get the config endpoint,
	// we cannot add a default prefix.
	D_ASSERT(prefix.empty());

	// For AWS Glue, ":" means "default account catalog" — omit the warehouse param
	string effective_warehouse = warehouse;
	if (endpoint_type == IcebergEndpointType::AWS_GLUE && warehouse == ":") {
		effective_warehouse = "";
	}
	auto catalog_config = IRCAPI::GetCatalogConfig(context, *this, effective_warehouse);
	overrides = catalog_config.overrides;
	defaults = catalog_config.defaults;
	auto uri_override_it = overrides.find("uri");
	if (uri_override_it != overrides.end()) {
		uri = uri_override_it->second;
		StringUtil::RTrim(uri, "/");
	}
	ParsePrefix();
	ParseNamespaceSeparator();

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
}

//===--------------------------------------------------------------------===//
// Attach
//===--------------------------------------------------------------------===//

//! Streamlined initialization for recognized catalog types

void IcebergCatalog::SetAttachOptions(const unordered_map<string, Value> &options) {
	raw_attach_options.insert(options.begin(), options.end());
}

bool IcebergCatalog::HasConflictingAttachOptions(const string &path, const AttachOptions &options) {
	//! If the base catalog already considers the path or catalog type to conflict, re-attach.
	if (Catalog::HasConflictingAttachOptions(path, options)) {
		return true;
	}
	//! Otherwise compare the iceberg-specific attach options (endpoint, credentials, MAX_TABLE_STALENESS, ...)
	//! so that ATTACH OR REPLACE re-runs Attach when any of them changes.
	if (options.options.size() != raw_attach_options.size()) {
		return true;
	}
	for (auto &entry : options.options) {
		auto it = raw_attach_options.find(entry.first);
		if (it == raw_attach_options.end()) {
			return true;
		}
		if (it->second.type() != entry.second.type() || it->second.ToString() != entry.second.ToString()) {
			return true;
		}
	}
	return false;
}

string IcebergCatalog::GetOnlyMergeOnReadSupportedErrorMessage(const string &table_name, const string &property,
                                                               const string &property_value) {
	return StringUtil::Format("DuckDB-Iceberg only supports merge-on-read for updates/deletes. Table Property '%s' is "
	                          "set to '%s' for table %s"
	                          "You can modify Iceberg table properties wth the set_iceberg_table_properties() "
	                          "function, and remove them with the remove_iceberg_table_properties() function. "
	                          "You can view Iceberg table properties with the iceberg_table_properties() function",
	                          property, property_value, table_name);
}

} // namespace duckdb
