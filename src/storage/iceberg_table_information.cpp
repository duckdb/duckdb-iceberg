#include "storage/iceberg_table_information.hpp"

#include "duckdb/common/string_util.hpp"
#include "storage/irc_transaction.hpp"
#include "storage/irc_schema_entry.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_authorization.hpp"
#include "storage/authorization/oauth2.hpp"
#include "storage/authorization/sigv4.hpp"
#include "storage/authorization/none.hpp"

namespace duckdb {

const string &IcebergTableInformation::BaseFilePath() const {
	return load_table_result.metadata.location;
}

static string DetectStorageType(const string &location) {
	// Detect storage type from the location URL
	if (StringUtil::StartsWith(location, "gs://") || StringUtil::Contains(location, "storage.googleapis.com")) {
		return "gcs";
	} else if (StringUtil::StartsWith(location, "s3://") || StringUtil::StartsWith(location, "s3a://")) {
		return "s3";
	} else if (StringUtil::StartsWith(location, "abfs://") || StringUtil::StartsWith(location, "az://")) {
		return "azure";
	}
	// Default to s3 for backward compatibility
	return "s3";
}

static void ParseGCSConfigOptions(const case_insensitive_map_t<string> &config,
                                  case_insensitive_map_t<Value> &options) {
	// Parse GCS-specific configuration.
	auto token_it = config.find("gcs.oauth2.token");
	if (token_it != config.end()) {
		options["bearer_token"] = token_it->second;
	}
}

static void ParseS3ConfigOptions(const case_insensitive_map_t<string> &config, case_insensitive_map_t<Value> &options) {
	// Set of recognized S3 config parameters and the duckdb secret option that matches it.
	static const case_insensitive_map_t<string> config_to_option = {{"s3.access-key-id", "key_id"},
	                                                                {"s3.secret-access-key", "secret"},
	                                                                {"s3.session-token", "session_token"},
	                                                                {"s3.region", "region"},
	                                                                {"region", "region"},
	                                                                {"client.region", "region"},
	                                                                {"s3.endpoint", "endpoint"}};

	for (auto &entry : config) {
		auto it = config_to_option.find(entry.first);
		if (it != config_to_option.end()) {
			options[it->second] = entry.second;
		}
	}
}

static void ParseConfigOptions(const case_insensitive_map_t<string> &config, case_insensitive_map_t<Value> &options,
                               const string &storage_type = "s3") {
	if (config.empty()) {
		return;
	}

	// Parse storage-specific config options
	if (storage_type == "gcs") {
		ParseGCSConfigOptions(config, options);
	} else {
		// Default to S3 parsing for backward compatibility
		ParseS3ConfigOptions(config, options);
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
		endpoint = endpoint.substr(7, string::npos);
	}
	if (StringUtil::StartsWith(endpoint, "https://")) {
		endpoint = endpoint.substr(8, string::npos);
		// if there is an endpoint and the endpoiont has https, use ssl.
		options["use_ssl"] = Value(true);
	}
	if (StringUtil::EndsWith(endpoint, "/")) {
		endpoint = endpoint.substr(0, endpoint.size() - 1);
	}
	endpoint_it->second = endpoint;
}

IRCAPITableCredentials IcebergTableInformation::GetVendedCredentials(ClientContext &context) {
	IRCAPITableCredentials result;

	auto transaction_id = MetaTransaction::Get(context).global_transaction_id;
	auto &transaction = IRCTransaction::Get(context, catalog);

	auto secret_base_name =
	    StringUtil::Format("__internal_ic_%s__%s__%s__%s", table_id, schema.name, name, to_string(transaction_id));
	transaction.created_secrets.insert(secret_base_name);
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

	// Detect storage type from metadata location
	const auto &metadata_location = load_table_result.metadata.location;
	string storage_type = DetectStorageType(metadata_location);

	// Mapping from config key to a duckdb secret option
	case_insensitive_map_t<Value> config_options;
	//! TODO: apply the 'defaults' retrieved from the /v1/config endpoint
	config_options.insert(user_defaults.begin(), user_defaults.end());

	if (load_table_result.has_config) {
		auto &config = load_table_result.config;
		ParseConfigOptions(config, config_options, storage_type);
	}

	if (load_table_result.has_storage_credentials) {
		auto &storage_credentials = load_table_result.storage_credentials;

		//! If there is only one credential listed, we don't really care about the prefix,
		//! we can use the metadata_location instead.
		const bool ignore_credential_prefix = storage_credentials.size() == 1;
		for (idx_t index = 0; index < storage_credentials.size(); index++) {
			auto &credential = storage_credentials[index];
			CreateSecretInput create_secret_input;
			create_secret_input.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
			create_secret_input.persist_type = SecretPersistType::TEMPORARY;

			create_secret_input.scope.push_back(ignore_credential_prefix ? metadata_location : credential.prefix);
			create_secret_input.name = StringUtil::Format("%s_%d_%s", secret_base_name, index, credential.prefix);

			create_secret_input.type = storage_type;
			create_secret_input.provider = "config";
			create_secret_input.storage_type = "memory";
			create_secret_input.options = config_options;

			ParseConfigOptions(credential.config, create_secret_input.options, storage_type);
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
		config.type = storage_type;
		config.provider = "config";
		config.storage_type = "memory";
	}

	return result;
}

optional_ptr<CatalogEntry> IcebergTableInformation::CreateSchemaVersion(IcebergTableSchema &table_schema) {
	CreateTableInfo info;
	info.table = name;
	for (auto &col : table_schema.columns) {
		info.columns.AddColumn(ColumnDefinition(col->name, col->type));
	}

	auto table_entry = make_uniq<ICTableEntry>(*this, catalog, schema, info);
	if (!table_entry->internal) {
		table_entry->internal = schema.internal;
	}
	auto result = table_entry.get();
	if (result->name.empty()) {
		throw InternalException("ICTableSet::CreateEntry called with empty name");
	}
	schema_versions.emplace(table_schema.schema_id, std::move(table_entry));
	return result;
}

optional_ptr<CatalogEntry> IcebergTableInformation::GetSchemaVersion(optional_ptr<BoundAtClause> at) {
	D_ASSERT(!schema_versions.empty());
	auto snapshot_lookup = IcebergSnapshotLookup::FromAtClause(at);

	int32_t schema_id;
	if (snapshot_lookup.IsLatest()) {
		schema_id = table_metadata.current_schema_id;
	} else {
		auto snapshot = table_metadata.GetSnapshot(snapshot_lookup);
		D_ASSERT(snapshot);
		schema_id = snapshot->schema_id;
	}
	return schema_versions[schema_id].get();
}

IcebergTableInformation::IcebergTableInformation(IRCatalog &catalog, IRCSchemaEntry &schema, const string &name)
    : catalog(catalog), schema(schema), name(name) {
	table_id = "uuid-" + schema.name + "-" + name;
}

void IcebergTableInformation::InitTransactionData(IRCTransaction &transaction) {
	if (!transaction_data) {
		auto context = transaction.context.lock();
		transaction_data = make_uniq<IcebergTransactionData>(*context, *this);
	}
}

void IcebergTableInformation::AddSnapshot(IRCTransaction &transaction, vector<IcebergManifestEntry> &&data_files) {
	D_ASSERT(!data_files.empty());
	InitTransactionData(transaction);
	transaction_data->AddSnapshot(IcebergSnapshotOperationType::APPEND, std::move(data_files));
}

void IcebergTableInformation::AddSchema(IRCTransaction &transaction) {
	InitTransactionData(transaction);
	transaction_data->TableAddSchema();
}

void IcebergTableInformation::AddAssignUUID(IRCTransaction &transaction) {
	InitTransactionData(transaction);
	transaction_data->TableAssignUUID();
}

void IcebergTableInformation::AddAssertCreate(IRCTransaction &transaction) {
	InitTransactionData(transaction);
	transaction_data->TableAddAssertCreate();
}

void IcebergTableInformation::AddUpradeFormatVersion(IRCTransaction &transaction) {
	InitTransactionData(transaction);
	transaction_data->TableAddUpradeFormatVersion();
}
void IcebergTableInformation::AddSetCurrentSchema(IRCTransaction &transaction) {
	InitTransactionData(transaction);
	transaction_data->TableAddSetCurrentSchema();
}
void IcebergTableInformation::AddPartitionSpec(IRCTransaction &transaction) {
	InitTransactionData(transaction);
	transaction_data->TableAddPartitionSpec();
}
void IcebergTableInformation::AddSortOrder(IRCTransaction &transaction) {
	InitTransactionData(transaction);
	transaction_data->TableAddSortOrder();
}
void IcebergTableInformation::SetDefaultSortOrder(IRCTransaction &transaction) {
	InitTransactionData(transaction);
	transaction_data->TableSetDefaultSortOrder();
}
void IcebergTableInformation::SetDefaultSpec(IRCTransaction &transaction) {
	InitTransactionData(transaction);
	transaction_data->TableSetDefaultSpec();
}
void IcebergTableInformation::SetProperties(IRCTransaction &transaction, case_insensitive_map_t<string> properties) {
	InitTransactionData(transaction);
	transaction_data->TableSetProperties(properties);
}
void IcebergTableInformation::SetLocation(IRCTransaction &transaction) {
	InitTransactionData(transaction);
	transaction_data->TableSetLocation();
}

} // namespace duckdb
