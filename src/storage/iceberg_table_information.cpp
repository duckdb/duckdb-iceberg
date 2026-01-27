#include "storage/iceberg_table_information.hpp"

#include "catalog_api.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "storage/irc_transaction.hpp"
#include "storage/iceberg_transaction_data.hpp"
#include "storage/irc_schema_entry.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_authorization.hpp"
#include "storage/authorization/oauth2.hpp"
#include "storage/authorization/sigv4.hpp"
#include "storage/authorization/none.hpp"

namespace duckdb {

const string &IcebergTableInformation::BaseFilePath() const {
	return table_metadata.location;
}

static string DetectStorageType(const string &location) {
	// Detect storage type from the location URL
	if (StringUtil::StartsWith(location, "gs://") || StringUtil::Contains(location, "storage.googleapis.com")) {
		return "gcs";
	} else if (StringUtil::StartsWith(location, "s3://") || StringUtil::StartsWith(location, "s3a://")) {
		return "s3";
	} else if (StringUtil::StartsWith(location, "abfs://") || StringUtil::StartsWith(location, "abfss://") ||
	           StringUtil::StartsWith(location, "az://")) {
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

static void ParseAzureConfigOptions(const case_insensitive_map_t<string> &config,
                                    case_insensitive_map_t<Value> &options) {
	static const string ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";

	for (auto &entry : config) {
		// SAS token config format is e.g. {adls.sas-token.<account-name>.dfs.core.windows.net, <token>}
		if (StringUtil::StartsWith(entry.first, ADLS_SAS_TOKEN_PREFIX)) {
			string host = entry.first.substr(ADLS_SAS_TOKEN_PREFIX.length());
			// Extract account name
			auto dot_pos = StringUtil::Find(host, ".");
			string account_name = dot_pos.IsValid() ? host.substr(0, dot_pos.GetIndex()) : host;

			if (!account_name.empty() && !entry.second.empty()) {
				options["account_name"] = account_name;
				options["connection_string"] =
				    StringUtil::Format("AccountName=%s;SharedAccessSignature=%s", account_name, entry.second);

				// For now, only process the first {storage account, token} pair we find in the config
				return;
			}
		}
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
	} else if (storage_type == "azure") {
		ParseAzureConfigOptions(config, options);
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
	const auto &table_location = table_metadata.GetLocation();
	string storage_type = DetectStorageType(table_location);

	// Mapping from config key to a duckdb secret option
	case_insensitive_map_t<Value> config_options;
	//! TODO: apply the 'defaults' retrieved from the /v1/config endpoint
	config_options.insert(user_defaults.begin(), user_defaults.end());
	auto key = IRCAPI::GetEncodedSchemaName(schema.namespace_items) + "." + name;
	auto &cached_value = catalog.GetLoadTableResult(key);
	auto &load_table_result = cached_value.load_table_result;
	if (load_table_result->has_config) {
		auto &config = load_table_result->config;
		ParseConfigOptions(config, config_options, storage_type);
	}

	if (load_table_result->has_storage_credentials) {
		auto &storage_credentials = load_table_result->storage_credentials;

		//! If there is only one credential listed, we don't really care about the prefix,
		//! we can use the table_location instead.
		const bool ignore_credential_prefix = storage_credentials.size() == 1;
		for (idx_t index = 0; index < storage_credentials.size(); index++) {
			auto &credential = storage_credentials[index];
			CreateSecretInput create_secret_input;
			create_secret_input.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
			create_secret_input.persist_type = SecretPersistType::TEMPORARY;

			create_secret_input.scope.push_back(ignore_credential_prefix ? table_location : credential.prefix);
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

optional_ptr<CatalogEntry> IcebergTableInformation::GetLatestSchema() {
	return GetSchemaVersion(nullptr);
}

string IcebergTableInformation::GetTableKey(const vector<string> &namespace_items, const string &table_name) {
	if (namespace_items.empty()) {
		return table_name;
	}
	return IRCAPI::GetEncodedSchemaName(namespace_items) + "." + table_name;
}

string IcebergTableInformation::GetTableKey() const {
	return GetTableKey(schema.namespace_items, name);
}

IcebergSnapshotLookup IcebergTableInformation::GetSnapshotLookup(IRCTransaction &irc_transaction) const {
	auto &context = *irc_transaction.context.lock();
	return GetSnapshotLookup(context);
}

IcebergSnapshotLookup IcebergTableInformation::GetSnapshotLookup(ClientContext &context) const {
	const auto table_name = name;
	auto &meta_transaction = MetaTransaction::Get(context);
	auto transaction_start = meta_transaction.GetCurrentTransactionStartTimestamp();
	auto start = timestamp_tz_t(transaction_start);
	BoundAtClause new_at_clause = BoundAtClause("timestamp", Value::TIMESTAMPTZ(start));
	auto new_lookup_storage = EntryLookupInfo(CatalogType::TABLE_ENTRY, table_name, new_at_clause, QueryErrorContext());

	auto at = new_lookup_storage.GetAtClause();
	auto snapshot_lookup = IcebergSnapshotLookup::FromAtClause(at);
	return snapshot_lookup;
}

bool IcebergTableInformation::TableIsEmpty(const IcebergSnapshotLookup &snapshot_lookup) const {
	// edge case tables before data is inserted. There is no snapshot information, so we defer to latest.
	if (table_metadata.snapshots.empty() && snapshot_lookup.snapshot_source == SnapshotSource::FROM_TIMESTAMP) {
		auto timestamp_millis = Timestamp::GetEpochMs(snapshot_lookup.snapshot_timestamp);
		if (timestamp_millis >= table_metadata.last_updated_ms) {
			// current table was made before the transaction but is empty.
			// you can return current table information in an as-is form
			return true;
		}
	}
	return false;
}

bool IcebergTableInformation::HasTransactionUpdates() {
	return transaction_data && (!transaction_data->updates.empty() || !transaction_data->requirements.empty());
}

IcebergTableInformation IcebergTableInformation::Copy() const {
	auto ret = IcebergTableInformation(catalog, schema, name);
	auto table_key = ret.GetTableKey();
	auto &cached_load_table_result = catalog.GetLoadTableResult(table_key);
	ret.table_metadata = IcebergTableMetadata::FromTableMetadata(cached_load_table_result.load_table_result->metadata);
	ret.table_metadata.latest_metadata_json = cached_load_table_result.load_table_result->metadata_location;
	return ret;
}

IcebergTableInformation IcebergTableInformation::Copy(IRCTransaction &irc_transaction) const {
	auto ret = Copy();
	// get snapshot from start of transaction
	// latest_snapshot_id and sequence of copied table information should be asof the transaction start
	// this is to ensure when the transaction commits, the assert ref snapshot id is the one closest to the start of
	// this
	auto snapshot_lookup = GetSnapshotLookup(irc_transaction);
	optional_ptr<IcebergSnapshot> snapshot = nullptr;
	try {
		snapshot = ret.table_metadata.GetSnapshot(snapshot_lookup);
	} catch (InvalidConfigurationException &e) {
		// lookup may fail for empty tables, since no snapshot exists
		if (ret.TableIsEmpty(snapshot_lookup)) {
			return ret;
		}
		throw TransactionException("Table %s is already outdated. Please restart your transaction", GetTableKey());
	}

	D_ASSERT(snapshot);
	ret.table_metadata.current_schema_id = snapshot->schema_id;
	ret.table_metadata.last_sequence_number = snapshot->sequence_number;
	ret.table_metadata.current_snapshot_id = snapshot->snapshot_id;
	return ret;
}

void IcebergTableInformation::InitSchemaVersions() {
	for (auto &table_schema : table_metadata.schemas) {
		CreateSchemaVersion(*table_schema.second);
	}
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

void IcebergTableInformation::AddDeleteSnapshot(IRCTransaction &transaction,
                                                vector<IcebergManifestEntry> &&data_files) {
	InitTransactionData(transaction);

	transaction_data->AddSnapshot(IcebergSnapshotOperationType::DELETE, std::move(data_files));
}

void IcebergTableInformation::AddUpdateSnapshot(IRCTransaction &transaction,
                                                vector<IcebergManifestEntry> &&delete_files,
                                                vector<IcebergManifestEntry> &&data_files) {
	InitTransactionData(transaction);
	// Automatically creates new snapshot with SnapshotOperationType::Overwrite
	transaction_data->AddUpdateSnapshot(std::move(delete_files), std::move(data_files));
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
void IcebergTableInformation::RemoveProperties(IRCTransaction &transaction, vector<string> properties) {
	InitTransactionData(transaction);
	transaction_data->TableRemoveProperties(properties);
}
void IcebergTableInformation::SetLocation(IRCTransaction &transaction) {
	InitTransactionData(transaction);
	transaction_data->TableSetLocation();
}

bool IcebergTableInformation::IsTransactionLocalTable(IRCTransaction &transaction) {
	for (auto &tbl : transaction.updated_tables) {
		if (tbl.first == GetTableKey()) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
