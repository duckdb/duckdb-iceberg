#include "storage/catalog/iceberg_schema_entry.hpp"
#include "storage/catalog/iceberg_table_entry.hpp"
#include "storage/iceberg_transaction.hpp"
#include "catalog_api.hpp"
#include "catalog_utils.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_logging.hpp"
#include "api_utils.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "rest_catalog/objects/catalog_config.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "storage/catalog/iceberg_catalog.hpp"
#include "regex"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "storage/iceberg_authorization.hpp"
#include "storage/authorization/oauth2.hpp"
#include "storage/authorization/sigv4.hpp"
#include "storage/authorization/none.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

IcebergCatalog::IcebergCatalog(AttachedDatabase &db_p, AccessMode access_mode,
                               unique_ptr<IcebergAuthorization> auth_handler, IcebergAttachOptions &attach_options,
                               const string &default_schema)
    : Catalog(db_p), access_mode(access_mode), auth_handler(std::move(auth_handler)),
      warehouse(attach_options.warehouse), uri(attach_options.endpoint), version("v1"), attach_options(attach_options),
      default_schema(default_schema), schemas(*this), metadata_cache() {
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

void IcebergCatalog::StoreLoadTableResult(const string &table_key,
                                          unique_ptr<const rest_api_objects::LoadTableResult> load_table_result) {
	std::lock_guard<std::mutex> g(metadata_cache_mutex);
	// erase load table result if it exists.
	if (metadata_cache.find(table_key) != metadata_cache.end()) {
		metadata_cache.erase(table_key);
	}
	// If max_table_staleness_minutes is not set, use a time in the past so cache is always expired
	system_clock::time_point expires_at;
	if (attach_options.max_table_staleness_micros.IsValid()) {
		expires_at =
		    system_clock::now() + std::chrono::microseconds(attach_options.max_table_staleness_micros.GetIndex());
	} else {
		expires_at = system_clock::time_point::min();
	}
	auto val = make_uniq<MetadataCacheValue>(expires_at, std::move(load_table_result));
	metadata_cache.emplace(table_key, std::move(val));
}

MetadataCacheValue &IcebergCatalog::GetLoadTableResult(const string &table_key) {
	std::lock_guard<std::mutex> g(metadata_cache_mutex);
	if (metadata_cache.find(table_key) == metadata_cache.end()) {
		throw InternalException("Attempting to retrieve table information that was never stored");
	}
	auto res = metadata_cache.find(table_key);
	D_ASSERT(res != metadata_cache.end());
	return *res->second;
}

std::mutex &IcebergCatalog::GetMetadataCacheLock() {
	return metadata_cache_mutex;
}

optional_ptr<MetadataCacheValue> IcebergCatalog::TryGetValidCachedLoadTableResult(const string &table_key,
                                                                                  lock_guard<std::mutex> &lock) {
	(void)lock;
	auto it = metadata_cache.find(table_key);
	if (it == metadata_cache.end()) {
		return nullptr;
	}
	auto &cached_value = *it->second;
	if (system_clock::now() > cached_value.expires_at) {
		// cached value has expired
		return nullptr;
	}
	return &cached_value;
}

void IcebergCatalog::RemoveLoadTableResult(const string &table_key) {
	std::lock_guard<std::mutex> g(metadata_cache_mutex);
	if (metadata_cache.find(table_key) == metadata_cache.end()) {
		throw InternalException("Attempting to remove table information that was never stored");
	}
	metadata_cache.erase(table_key);
}

optional_ptr<CatalogEntry> IcebergCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	optional_ptr<ClientContext> context = transaction.GetContext();
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException(
		    "CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements");
	}

	D_ASSERT(context);

	// Verify schema existence on the server first
	bool schema_exists = IRCAPI::VerifySchemaExistence(*context, *this, info.schema);

	if (schema_exists) {
		if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			// Schema already exists on the server - get or create a local entry and return it
			auto entry = schemas.GetEntry(*context, info.schema, OnEntryNotFound::RETURN_NULL);
			if (entry) {
				return entry;
			}
			auto new_schema = make_uniq<IcebergSchemaEntry>(*this, info);
			auto schema_name = new_schema->name;
			schemas.AddEntry(schema_name, std::move(new_schema));
			return &schemas.GetEntry(info.schema);
		}
		throw CatalogException("Schema with name \"%s\" already exists", info.schema);
	}

	// Schema does not exist - create it locally and defer the server creation to commit
	auto &iceberg_transaction = IcebergTransaction::Get(*context, *this);
	auto new_schema = make_uniq<IcebergSchemaEntry>(*this, info);
	auto schema_name = new_schema->name;
	schemas.AddEntry(schema_name, std::move(new_schema));
	iceberg_transaction.created_schemas.insert(info.schema);
	return &schemas.GetEntry(info.schema);
}

void IcebergCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	if (info.cascade) {
		throw NotImplementedException(
		    "DROP SCHEMA <schema_name> CASCADE is not supported for Iceberg schemas currently");
	}

	// Verify schema existence on the server first
	bool schema_exists = IRCAPI::VerifySchemaExistence(context, *this, info.name);

	if (!schema_exists) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Schema with name \"%s\" does not exist", info.name);
	}

	// Schema exists - defer the server deletion to commit
	auto &iceberg_transaction = IcebergTransaction::Get(context, *this);
	iceberg_transaction.deleted_schemas.insert(info.name);
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

//===--------------------------------------------------------------------===//
// Iceberg REST Catalog
//===--------------------------------------------------------------------===//

IRCEndpointBuilder IcebergCatalog::GetBaseUrl() const {
	auto base_url = IRCEndpointBuilder();
	base_url.SetHost(uri);
	base_url.AddPathComponent(version);

	return base_url;
}

unique_ptr<SecretEntry> IcebergCatalog::GetStorageSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	case_insensitive_set_t accepted_secret_types {"s3", "aws"};

	if (!secret_name.empty()) {
		auto secret_entry = context.db->GetSecretManager().GetSecretByName(transaction, secret_name);
		if (secret_entry) {
			auto secret_type = secret_entry->secret->GetType();
			if (accepted_secret_types.count(secret_type)) {
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

	string decoded_prefix = "";
	if (default_prefix_it != defaults.end()) {
		decoded_prefix = StringUtil::URLDecode(default_prefix_it->second);
		prefix = decoded_prefix;
		if (!StringUtil::Equals(decoded_prefix, default_prefix_it->second)) {
			// decoded prefix contains a slash AND is not equal to the original
			// means prefix was encoded, and is one URL component
			prefix_is_one_component = true;
		} else {
			prefix_is_one_component = false;
		}
	}

	// sometimes the prefix in the overrides. Prefer the override prefix
	if (override_prefix_it != overrides.end()) {
		decoded_prefix = StringUtil::URLDecode(override_prefix_it->second);
		prefix = decoded_prefix;

		if (!StringUtil::Equals(decoded_prefix, override_prefix_it->second)) {
			// decoded prefix is not equal to the original
			// means prefix was encoded, and is one URL component
			prefix_is_one_component = true;
		} else {
			// decoded prefix contains a '/' or is equal
			prefix_is_one_component = false;
		}
	}
}

void IcebergCatalog::GetConfig(ClientContext &context, IcebergEndpointType &endpoint_type) {
	// set the prefix to be empty. To get the config endpoint,
	// we cannot add a default prefix.
	D_ASSERT(prefix.empty());
	auto catalog_config = IRCAPI::GetCatalogConfig(context, *this);
	overrides = catalog_config.overrides;
	defaults = catalog_config.defaults;
	ParsePrefix();

	if (attach_options.encode_entire_prefix) {
		prefix_is_one_component = true;
	}

	if (catalog_config.has_endpoints) {
		for (auto &endpoint : catalog_config.endpoints) {
			supported_urls.insert(endpoint);
		}
	}
	// should be if s3tables
	if (!catalog_config.has_endpoints && endpoint_type == IcebergEndpointType::AWS_S3TABLES) {
		supported_urls.clear();
		AddS3TablesEndpoints();
	} else if (!catalog_config.has_endpoints && endpoint_type == IcebergEndpointType::AWS_GLUE) {
		supported_urls.clear();
		AddGlueEndpoints();
	} else if (!catalog_config.has_endpoints) {
		AddDefaultSupportedEndpoints();
	}

	if (prefix.empty()) {
		DUCKDB_LOG(context, IcebergLogType, "No prefix found for catalog with warehouse value %s", warehouse);
	}
}

//===--------------------------------------------------------------------===//
// Attach
//===--------------------------------------------------------------------===//

// namespace
namespace {

static IcebergEndpointType EndpointTypeFromString(const string &input) {
	D_ASSERT(StringUtil::Lower(input) == input);

	static const case_insensitive_map_t<IcebergEndpointType> mapping {{"glue", IcebergEndpointType::AWS_GLUE},
	                                                                  {"s3_tables", IcebergEndpointType::AWS_S3TABLES}};

	for (auto &entry : mapping) {
		if (entry.first == input) {
			return entry.second;
		}
	}
	set<string> options;
	for (auto &entry : mapping) {
		options.insert(entry.first);
	}
	throw InvalidConfigurationException("Unrecognized 'endpoint_type' (%s), accepted options are: %s", input,
	                                    StringUtil::Join(options, ", "));
}

} // namespace

//! Streamlined initialization for recognized catalog types

static void S3OrGlueAttachInternal(IcebergAttachOptions &input, const string &service, const string &region) {
	if (input.authorization_type != IcebergAuthorizationType::INVALID) {
		throw InvalidConfigurationException("'endpoint_type' can not be combined with 'authorization_type'");
	}

	input.authorization_type = IcebergAuthorizationType::SIGV4;
	input.endpoint = StringUtil::Format("%s.%s.amazonaws.com/iceberg", service, region);
}

static void S3TablesAttach(IcebergAttachOptions &input) {
	// extract region from the amazon ARN
	auto substrings = StringUtil::Split(input.warehouse, ":");
	if (substrings.size() != 6) {
		throw InvalidInputException("Could not parse S3 Tables ARN warehouse value");
	}
	auto region = substrings[3];
	S3OrGlueAttachInternal(input, "s3tables", region);
}

static bool SanityCheckGlueWarehouse(const string &warehouse) {
	// See: https://docs.aws.amazon.com/glue/latest/dg/connect-glu-iceberg-rest.html#prefix-catalog-path-parameters

	const std::regex patterns[] = {
	    std::regex("^:$"),                  // Default catalog ":" in current account
	    std::regex("^\\d{12}$"),            // Default catalog in a specific account
	    std::regex("^\\d{12}:[^:/]+$"),     // Specific catalog in a specific account
	    std::regex("^[^:]+/[^:]+$"),        // Nested catalog in the current account
	    std::regex("^\\d{12}:[^/]+/[^:]+$") // Nested catalog in a specific account
	};

	for (const auto &pattern : patterns) {
		if (std::regex_match(warehouse, pattern)) {
			return true;
		}
	}

	throw InvalidConfigurationException(
	    "Invalid Glue Catalog Format: '%s'. Expected format: ':', '12-digit account ID', "
	    "'catalog1/catalog2', or '12-digit accountId:catalog1/catalog2'.",
	    warehouse);
}

static void GlueAttach(ClientContext &context, IcebergAttachOptions &input) {
	SanityCheckGlueWarehouse(input.warehouse);

	string secret;
	auto secret_it = input.options.find("secret");
	if (secret_it != input.options.end()) {
		secret = secret_it->second.ToString();
	}

	// look up any s3 secret

	// if there is no secret, an error will be thrown
	auto secret_entry = IcebergCatalog::GetStorageSecret(context, secret);
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	auto region = kv_secret.TryGetValue("region");

	if (region.IsNull()) {
		throw InvalidConfigurationException("Assumed catalog secret '%s' for catalog '%s' does not have a region",
		                                    secret_entry->secret->GetName(), input.name);
	}
	S3OrGlueAttachInternal(input, "glue", region.ToString());
}

void IcebergCatalog::SetAWSCatalogOptions(IcebergAttachOptions &attach_options,
                                          case_insensitive_set_t &set_by_attach_options) {
	attach_options.allows_deletes = false;
	if (set_by_attach_options.find("support_stage_create") == set_by_attach_options.end()) {
		attach_options.supports_stage_create = false;
	}
	if (set_by_attach_options.find("purge_requested") == set_by_attach_options.end()) {
		attach_options.purge_requested = true;
	}
}

unique_ptr<Catalog> IcebergCatalog::Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                           AttachedDatabase &db, const string &name, AttachInfo &info,
                                           AttachOptions &options) {
	IRCEndpointBuilder endpoint_builder;

	string endpoint_type_string;
	string authorization_type_string;
	string access_mode_string;

	IcebergAttachOptions attach_options;
	attach_options.warehouse = info.path;
	attach_options.name = name;

	// check if we have a secret provided
	string secret_name;
	string default_schema;
	case_insensitive_set_t set_by_attach_options;
	//! First handle generic attach options
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			continue;
		}

		if (lower_name == "endpoint_type") {
			endpoint_type_string = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "authorization_type") {
			authorization_type_string = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "access_delegation_mode") {
			access_mode_string = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "endpoint") {
			attach_options.endpoint = StringUtil::Lower(entry.second.ToString());
			StringUtil::RTrim(attach_options.endpoint, "/");
		} else if (lower_name == "support_stage_create") {
			auto result = entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
			attach_options.supports_stage_create = result;
			set_by_attach_options.insert("supports_stage_create");
		} else if (lower_name == "support_nested_namespaces") {
			attach_options.support_nested_namespaces =
			    entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
			set_by_attach_options.insert("support_nested_namespaces");
		} else if (lower_name == "purge_requested") {
			attach_options.purge_requested = entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
			set_by_attach_options.insert("purge_requested");
		} else if (lower_name == "default_schema") {
			default_schema = entry.second.ToString();
		} else if (lower_name == "encode_entire_prefix") {
			attach_options.encode_entire_prefix = true;
		} else if (lower_name == "max_table_staleness") {
			auto interval_option = entry.second.DefaultCastAs(LogicalType::INTERVAL);
			auto interval_value = interval_option.GetValue<interval_t>();
			int64_t interval_in_micros = 0;
			if (!Interval::TryGetMicro(interval_value, interval_in_micros)) {
				throw ConversionException("Could not get interval information from %s", interval_option.ToString());
			}
			attach_options.max_table_staleness_micros = interval_in_micros;
		} else {
			attach_options.options.emplace(std::move(entry));
		}
	}
	IcebergEndpointType endpoint_type = IcebergEndpointType::INVALID;
	//! Then check any if the 'endpoint_type' is set, for any well known catalogs
	if (!endpoint_type_string.empty()) {
		endpoint_type = EndpointTypeFromString(endpoint_type_string);
		switch (endpoint_type) {
		case IcebergEndpointType::AWS_GLUE: {
			GlueAttach(context, attach_options);
			endpoint_type = IcebergEndpointType::AWS_GLUE;
			SetAWSCatalogOptions(attach_options, set_by_attach_options);
			break;
		}
		case IcebergEndpointType::AWS_S3TABLES: {
			S3TablesAttach(attach_options);
			endpoint_type = IcebergEndpointType::AWS_S3TABLES;
			SetAWSCatalogOptions(attach_options, set_by_attach_options);
			break;
		}
		default:
			throw InternalException("Endpoint type (%s) not implemented", endpoint_type_string);
		}
	}

	//! Then check the authorization type
	if (!authorization_type_string.empty()) {
		if (attach_options.authorization_type != IcebergAuthorizationType::INVALID) {
			throw InvalidConfigurationException("'authorization_type' can not be combined with 'endpoint_type'");
		}
		attach_options.authorization_type = IcebergAuthorization::TypeFromString(authorization_type_string);
	}
	if (!access_mode_string.empty()) {
		if (access_mode_string == "vended_credentials") {
			attach_options.access_mode = IRCAccessDelegationMode::VENDED_CREDENTIALS;
		} else if (access_mode_string == "none") {
			attach_options.access_mode = IRCAccessDelegationMode::NONE;
		} else {
			throw InvalidInputException(
			    "Unrecognized access mode '%s'. Supported options are 'vended_credentials' and 'none'",
			    access_mode_string);
		}
	}
	if (attach_options.authorization_type == IcebergAuthorizationType::INVALID) {
		attach_options.authorization_type = IcebergAuthorizationType::OAUTH2;
	}

	//! Finally, create the auth_handler class from the authorization_type and the remaining options
	unique_ptr<IcebergAuthorization> auth_handler;
	switch (attach_options.authorization_type) {
	case IcebergAuthorizationType::OAUTH2: {
		auth_handler = OAuth2Authorization::FromAttachOptions(context, attach_options);
		break;
	}
	case IcebergAuthorizationType::SIGV4: {
		auth_handler = SIGV4Authorization::FromAttachOptions(attach_options);
		break;
	}
	case IcebergAuthorizationType::NONE: {
		auth_handler = NoneAuthorization::FromAttachOptions(attach_options);
		break;
	}
	default:
		throw InternalException("Authorization Type (%s) not implemented", authorization_type_string);
	}

	//! We throw if there are any additional options not handled by previous steps
	if (!attach_options.options.empty()) {
		set<string> unrecognized_options;
		for (auto &entry : attach_options.options) {
			unrecognized_options.insert(entry.first);
		}
		throw InvalidConfigurationException("Unhandled options found: %s",
		                                    StringUtil::Join(unrecognized_options, ", "));
	}

	if (attach_options.endpoint.empty()) {
		throw InvalidConfigurationException("Missing 'endpoint' option for Iceberg attach");
	}

	D_ASSERT(auth_handler);
	auto catalog =
	    make_uniq<IcebergCatalog>(db, options.access_mode, std::move(auth_handler), attach_options, default_schema);
	catalog->GetConfig(context, endpoint_type);
	return std::move(catalog);
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
