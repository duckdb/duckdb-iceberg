#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/types/string.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "catalog/rest/catalog_entry/iceberg_schema_entry.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "catalog/rest/storage/authorization/oauth2.hpp"
#include "catalog/rest/storage/authorization/sigv4.hpp"
#include "catalog/rest/storage/authorization/none.hpp"
#include "catalog/rest/storage/authorization/sigv4_utils.hpp"
#include "core/expression/iceberg_transform.hpp"

#include <climits>

namespace duckdb {

const string &IcebergTableInformation::BaseFilePath() const {
	return table_metadata.location;
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
string RetrieveRegion(DBConfig &db_config) {
	char *retrieved_region = std::getenv("AWS_REGION");
	if (retrieved_region) {
		return string(retrieved_region);
	}
	retrieved_region = std::getenv("AWS_DEFAULT_REGION");
	if (retrieved_region) {
		return string(retrieved_region);
	}

	Value region_value;
	if (db_config.TryGetCurrentSetting("s3_region", region_value)) {
		return region_value.ToString();
	}

	return "";
}
static void ParseConfigOptions(const case_insensitive_map_t<string> &config, case_insensitive_map_t<Value> &options,
                               ClientContext &context, const string &storage_type = "s3") {
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

		if (options.find("region") == options.end()) {
			const string region = RetrieveRegion(DBConfig::GetConfig(context));

			if (region.empty()) {
				throw InvalidConfigurationException(
				    "No region was provided via the vended credentials, and no region could be found via "
				    "environment variables. Please provide a default_region for the Iceberg Catalog when attaching");
			}
			options["region"] = Value(region);
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
	auto &transaction = IcebergTransaction::Get(context, catalog);

	auto secret_base_name =
	    StringUtil::Format("__internal_ic_%s__%s__%s__%s", table_id, schema.name, name, to_string(transaction_id));
	transaction.created_secrets.insert(secret_base_name);
	case_insensitive_map_t<Value> user_defaults;
	if (catalog.auth_handler->type == IcebergAuthorizationType::SIGV4) {
		auto &sigv4_auth = catalog.auth_handler->Cast<SIGV4Authorization>();
		auto catalog_credentials = IcebergCatalog::GetStorageSecret(context, sigv4_auth.secret);
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
		if (!sigv4_auth.sigv4_region.empty()) {
			user_defaults.emplace("region", Value(sigv4_auth.sigv4_region));
		}
	} else if (catalog.auth_handler->type == IcebergAuthorizationType::OAUTH2) {
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
	auto schema_component = IRCPathComponent::NamespaceComponent(schema.namespace_items);
	auto key = schema_component.encoded + "." + name;

	ParseConfigOptions(config, config_options, context, storage_type);

	//! If there is only one credential listed, we don't really care about the prefix,
	//! we can use the table_location instead.
	const bool ignore_credential_prefix = storage_credentials.size() == 1;
	for (idx_t index = 0; index < storage_credentials.size(); index++) {
		auto &credential = storage_credentials[index];

		//! Only use credentials whose prefix matches the storage type (e.g. "s3"),
		//! matching Iceberg Java S3FileIO behavior: filter(c -> c.prefix().startsWith(ROOT_PREFIX))
		if (!ignore_credential_prefix &&
		    !CredentialMatchesStorageType(credential.prefix, storage_type)) {
			continue;
		}

		CreateSecretInput create_secret_input;
		create_secret_input.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
		create_secret_input.persist_type = SecretPersistType::TEMPORARY;

		if (ignore_credential_prefix) {
			create_secret_input.scope.push_back(table_location);
		} else {
			create_secret_input.scope.push_back(credential.prefix);
			//! Also match paths whose scheme differs from the credential prefix
			//! (e.g. oss:// files with s3 credentials), equivalent to Java S3FileIO's
			//! ROOT_PREFIX fallback in clientForStoragePath()
			if (!StringUtil::StartsWith(table_location, credential.prefix)) {
				create_secret_input.scope.push_back(table_location);
			}
		}
		create_secret_input.name = StringUtil::Format("%s_%d_%s", secret_base_name, index, credential.prefix);

		create_secret_input.type = storage_type;
		create_secret_input.provider = "config";
		create_secret_input.storage_type = "memory";
		create_secret_input.options = config_options;

		ParseConfigOptions(credential.config, create_secret_input.options, context, storage_type);
		//! TODO: apply the 'overrides' retrieved from the /v1/config endpoint
		result.storage_credentials.push_back(create_secret_input);
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

optional_ptr<CatalogEntry> IcebergTableInformation::CreateSchemaVersion(const IcebergTableSchema &table_schema) {
	CreateTableInfo info;
	info.table = name;
	for (auto &col : table_schema.columns) {
		info.columns.AddColumn(col->GetColumnDefinition());
	}

	auto table_entry = make_uniq<IcebergTableEntry>(*this, catalog, schema, info, table_schema.schema_id);
	if (!table_entry->internal) {
		table_entry->internal = schema.internal;
	}
	auto result = table_entry.get();
	if (result->name.empty()) {
		throw InternalException("IcebergTableSet::CreateEntry called with empty name");
	}

	schema_versions.emplace(table_schema.schema_id, std::move(table_entry));
	return result;
}

idx_t IcebergTableInformation::GetMaxSchemaId() {
	idx_t max_schema_id = 0;
	if (schema_versions.empty()) {
		throw CatalogException("No schema versions found for table '%s.%s'", schema.name, name);
	}
	for (auto &schema : schema_versions) {
		if (schema.first > max_schema_id) {
			max_schema_id = schema.first;
		}
	}
	return max_schema_id;
}

idx_t IcebergTableInformation::GetNextPartitionSpecId() {
	idx_t max_partition_spec_id = table_metadata.default_spec_id;
	for (auto &schema : table_metadata.GetPartitionSpecs()) {
		if (schema.first > max_partition_spec_id) {
			max_partition_spec_id = schema.first;
		}
	}
	return max_partition_spec_id + 1;
}

int64_t IcebergTableInformation::GetExistingSpecId(IcebergPartitionSpec &spec) {
	int64_t existing_spec_id = -1;
	for (auto &existing_spec : table_metadata.GetPartitionSpecs()) {
		bool fields_match = true;
		if (existing_spec.second.fields.size() != spec.fields.size()) {
			continue;
		}
		for (idx_t field_index = 0; field_index < existing_spec.second.fields.size(); field_index++) {
			auto existing_partition_col_source_id = existing_spec.second.fields[field_index].source_id;
			// if the number of partition columns don't match, the specs are not the same
			auto new_spec_col_source_id = spec.fields[field_index].source_id;
			if (existing_partition_col_source_id != new_spec_col_source_id) {
				fields_match = false;
				break;
			}
			auto existing_partition_col_transform = existing_spec.second.fields[field_index].transform.RawType();
			auto new_spec_col_transform = spec.fields[field_index].transform.RawType();
			if (existing_partition_col_transform != new_spec_col_transform) {
				fields_match = false;
				break;
			}
		}
		if (!fields_match) {
			continue;
		}
		// source ids are the same, transforms are the same, and partition amount is the same
		// so we just use the existing spec.
		existing_spec_id = existing_spec.second.spec_id;
		break;
	}
	return existing_spec_id;
}
void IcebergTableInformation::SetPartitionedBy(IcebergTransaction &transaction,
                                               const vector<unique_ptr<ParsedExpression>> &partition_keys,
                                               const IcebergTableSchema &schema, bool first_partition_spec) {
	idx_t base_partition_field_id = 1000;
	if (!first_partition_spec && table_metadata.HasLastPartitionId()) {
		base_partition_field_id = table_metadata.GetLastPartitionFieldId() + 1;
	}

	idx_t new_spec_id = 0;
	if (!first_partition_spec) {
		new_spec_id = GetNextPartitionSpecId();
	}
	auto &transaction_data = GetOrCreateTransactionData(transaction);

	IcebergPartitionSpec new_spec(new_spec_id);

	for (auto &key : partition_keys) {
		string column_name;
		string transform = "identity";
		idx_t bucket_modulo_val;

		if (key->type == ExpressionType::COLUMN_REF) {
			auto &colref = key->Cast<ColumnRefExpression>();
			column_name = colref.column_names.back();
		} else if (key->type == ExpressionType::FUNCTION) {
			auto &funcexpr = key->Cast<FunctionExpression>();
			transform = funcexpr.function_name;
			if (funcexpr.children.empty()) {
				throw NotImplementedException("Unrecognized transform ('%s')", transform);
			} else if (!IcebergTransform::TransformFunctionSupported(transform)) {
				throw NotImplementedException("Unrecognized transform ('%s')", transform);
			}
			if (transform == "bucket" || transform == "truncate") {
				// Spark-compatible syntax: bucket(N, col) / truncate(W, col)
				if (funcexpr.children.size() < 2) {
					throw InvalidInputException("%s requires two arguments, e.g. %s(16, col)", transform, transform);
				}
				auto &param_expr = *funcexpr.children[0];
				if (param_expr.type != ExpressionType::VALUE_CONSTANT) {
					throw InvalidInputException("%s first argument must be a constant integer", transform);
				}
				auto &const_expr = param_expr.Cast<ConstantExpression>();
				auto raw_val = const_expr.value.GetValue<int32_t>();
				if (raw_val <= 0) {
					throw InvalidInputException("%s requires a positive integer argument, got %d", transform, raw_val);
				}
				bucket_modulo_val = const_expr.value.GetValue<idx_t>();
				transform = StringUtil::Format("%s[%d]", transform, bucket_modulo_val);
				if (funcexpr.children[1]->type != ExpressionType::COLUMN_REF) {
					throw NotImplementedException("Transforms are only supported on column references, not %s",
					                              EnumUtil::ToChars(funcexpr.children[1]->type));
				}
				auto &colref = funcexpr.children[1]->Cast<ColumnRefExpression>();
				column_name = colref.column_names.back();
			} else {
				if (funcexpr.children[0]->type != ExpressionType::COLUMN_REF) {
					throw NotImplementedException("Transforms are only supported on column references, not %s",
					                              EnumUtil::ToChars(funcexpr.children[0]->type));
				}
				auto &colref = funcexpr.children[0]->Cast<ColumnRefExpression>();
				column_name = colref.column_names.back();
			}
		} else {
			throw NotImplementedException("Unsupported partition key type: %s", key->ToString());
		}

		// Find source_id
		int32_t source_id = -1;
		for (auto &col : schema.columns) {
			if (StringUtil::CIEquals(col->name, column_name)) {
				source_id = col->id;
				break;
			}
		}
		if (source_id == -1) {
			throw CatalogException("Column \"%s\" not found in schema", column_name);
		}

		IcebergPartitionSpecField field;
		// Create the Iceberg transform
		auto iceberg_transform = IcebergTransform(transform);
		switch (iceberg_transform.Type()) {
		case IcebergTransformType::BUCKET:
		case IcebergTransformType::TRUNCATE:
			iceberg_transform.SetBucketOrTruncateValue(bucket_modulo_val);
			break;
		default:
			break;
		}
		field.transform = iceberg_transform;
		field.source_id = source_id;
		field.partition_field_id = base_partition_field_id + new_spec.fields.size();
		// transform field names cannot be the column name. Otherwise Lakekeeper complains
		field.SetPartitionSpecFieldName(column_name);
		new_spec.fields.push_back(std::move(field));
	}

	// if spec definition already exists in a previous spec definition, set it to that spec id
	// (some catalog may allow duplicate definitions, others not)
	int64_t existing_spec_id = GetExistingSpecId(new_spec);
	if (existing_spec_id >= 0) {
		table_metadata.default_spec_id = existing_spec_id;
		transaction_data.TableSetDefaultSpec();
		return;
	}

	table_metadata.partition_specs.emplace(new_spec_id, std::move(new_spec));
	table_metadata.default_spec_id = new_spec_id;
	if (!first_partition_spec) {
		transaction_data.TableAddPartitionSpec();
		transaction_data.TableSetDefaultSpec();
	}
}

optional_ptr<CatalogEntry> IcebergTableInformation::GetSchemaVersion(ClientContext &context,
                                                                     optional_ptr<BoundAtClause> at) {
	if (table_metadata.snapshots.empty()) {
		return schema_versions[table_metadata.GetCurrentSchemaId()].get();
	}

	D_ASSERT(!schema_versions.empty());
	auto &meta_transaction = MetaTransaction::Get(context);
	auto transaction_start = meta_transaction.GetCurrentTransactionStartTimestamp();
	auto transaction_start_millis = Timestamp::GetEpochMs(transaction_start);

	auto snapshot_lookup = IcebergSnapshotLookup::FromAtClause(at);
	auto snapshot_info = table_metadata.GetSnapshot(snapshot_lookup);

	int32_t schema_id;
	if (!snapshot_lookup.IsLatest() && snapshot_info.snapshot) {
		//! Time travel query, verify this is reachable
		auto &snapshot = *snapshot_info.snapshot;
		if (snapshot.timestamp_ms.value > transaction_start.value) {
			//! Not reachable by the current transaction
			return nullptr;
		}
		schema_id = snapshot.GetSchemaId();
	} else {
		bool use_metadata_log = true;
		Value val;
		if (context.TryGetCurrentSetting("iceberg_use_metadata_log", val)) {
			if (!val.IsNull() && val.type().id() == LogicalTypeId::BOOLEAN) {
				use_metadata_log = val.GetValue<bool>();
			}
		}

		const bool latest_metadata_is_too_fresh = table_metadata.last_updated_ms.value > transaction_start_millis;
		const bool can_use_metadata_log = use_metadata_log && !table_metadata.metadata_log.empty();
		if (latest_metadata_is_too_fresh && can_use_metadata_log) {
			string metadata_path;
			auto relevant_metadata = CreateMetadataFromLog(context, transaction_start_millis, metadata_path);
			schema_id = relevant_metadata.GetCurrentSchemaId();
		} else {
			schema_id = table_metadata.GetCurrentSchemaId();
		}
	}
	return schema_versions[schema_id].get();
}

idx_t IcebergTableInformation::GetIcebergVersion() const {
	return table_metadata.iceberg_version;
}

optional_ptr<CatalogEntry> IcebergTableInformation::GetLatestSchema(ClientContext &context) {
	return GetSchemaVersion(context, nullptr);
}

string IcebergTableInformation::GetTableKey(const vector<string> &namespace_items, const string &table_name) {
	if (namespace_items.empty()) {
		return table_name;
	}
	auto schema_component = IRCPathComponent::NamespaceComponent(namespace_items);
	return schema_component.encoded + "." + table_name;
}

string IcebergTableInformation::GetTableKey() const {
	return GetTableKey(schema.namespace_items, name);
}

IcebergSnapshotLookup IcebergTableInformation::GetSnapshotLookup(IcebergTransaction &iceberg_transaction) const {
	auto locked_context = iceberg_transaction.context.lock();
	auto &context = *locked_context;
	return GetSnapshotLookup(context);
}

IcebergSnapshotLookup IcebergTableInformation::GetSnapshotLookup(ClientContext &context,
                                                                 optional_ptr<BoundAtClause> at) const {
	if (!at && !HasTransactionUpdates()) {
		// if there is no user supplied AT () clause, and the table does not have transaction updates
		// use transaction start time
		return GetSnapshotLookup(context);
	}
	return IcebergSnapshotLookup::FromAtClause(at);
}

IcebergSnapshotLookup IcebergTableInformation::GetSnapshotLookup(ClientContext &context) const {
	auto &meta_transaction = MetaTransaction::Get(context);
	auto transaction_start = meta_transaction.GetCurrentTransactionStartTimestamp();

	IcebergSnapshotLookup res;
	res.snapshot_timestamp = transaction_start;
	res.SetSource(SnapshotSource::FROM_TIMESTAMP);
	return res;
}

bool IcebergTableInformation::TableIsEmpty(const IcebergSnapshotLookup &snapshot_lookup) const {
	// edge case tables before data is inserted. There is no snapshot information, so we defer to latest.
	if (table_metadata.snapshots.empty() && snapshot_lookup.IsFromTimestamp()) {
		auto timestamp_millis = timestamp_t(Timestamp::GetEpochMs(snapshot_lookup.snapshot_timestamp));
		if (timestamp_millis >= table_metadata.last_updated_ms) {
			// current table was made before the transaction but is empty.
			// you can return current table information in an as-is form
			return true;
		}
	}
	return false;
}

bool IcebergTableInformation::HasTransactionUpdates() const {
	if (!transaction_data) {
		return false;
	}
	auto &data = *transaction_data;
	if (!data.updates.empty()) {
		return true;
	}
	if (!data.requirements.empty()) {
		return true;
	}
	if (data.set_schema_id) {
		return true;
	}
	if (data.assert_schema_id) {
		return true;
	}
	return false;
}

IcebergTableInformation IcebergTableInformation::Copy(ClientContext &context) const {
	auto ret = IcebergTableInformation(catalog, schema, name);
	auto table_key = ret.GetTableKey();
	{
		lock_guard<std::mutex> cache_lock(catalog.table_request_cache.Lock());
		auto cached_result = catalog.table_request_cache.Get(context, table_key, cache_lock, false);
		D_ASSERT(cached_result);
		auto &cached_table_result = *cached_result->load_table_result;
		ret.InitializeFromLoadTableResult(cached_table_result, false);
	}
	return ret;
}

IcebergTableMetadata IcebergTableInformation::CreateMetadataFromLog(ClientContext &context,
                                                                    int64_t transaction_start_millis,
                                                                    string &metadata_path) const {
	auto &log = table_metadata.metadata_log;

	optional_idx log_item_index;
	for (idx_t i = log.size(); i-- > 0;) {
		if (log[i].timestamp_ms <= transaction_start_millis) {
			log_item_index = i;
			break;
		}
	}
	if (!log_item_index.IsValid()) {
		throw InternalException(
		    "Metadata-log exists but none of the entries were valid for the current transaction start time (%s)",
		    Timestamp::ToString(timestamp_ms_t(transaction_start_millis)));
	}

	auto fs = make_shared_ptr<CachingFileSystemWrapper>(FileSystem::GetFileSystem(context), *context.db);
	auto &path = log[log_item_index.GetIndex()].metadata_file;
	auto parsed_metadata = IcebergTableMetadata::Parse(path, *fs, "");

	metadata_path = path;
	return IcebergTableMetadata::FromTableMetadata(parsed_metadata);
}

IcebergTableInformation IcebergTableInformation::Copy(IcebergTransaction &iceberg_transaction) const {
	auto locked_context = iceberg_transaction.context.lock();
	auto &context = *locked_context;

	auto ret = Copy(context);
	auto &meta_transaction = MetaTransaction::Get(context);
	auto transaction_start = meta_transaction.GetCurrentTransactionStartTimestamp();
	auto transaction_start_millis = Timestamp::GetEpochMs(transaction_start);

	if (table_metadata.last_updated_ms.value > transaction_start_millis) {
		bool use_metadata_log = true;
		Value val;
		if (context.TryGetCurrentSetting("iceberg_use_metadata_log", val)) {
			if (!val.IsNull() && val.type().id() == LogicalTypeId::BOOLEAN) {
				use_metadata_log = val.GetValue<bool>();
			}
		}

		const bool can_use_metadata_log = use_metadata_log && !table_metadata.metadata_log.empty();
		if (!can_use_metadata_log) {
			auto snapshot_lookup = GetSnapshotLookup(iceberg_transaction);
			if (ret.TableIsEmpty(snapshot_lookup)) {
				return ret;
			}
			IcebergSnapshotScanInfo snapshot_info;
			snapshot_info = ret.table_metadata.GetSnapshot(snapshot_lookup);
			if (!snapshot_info.snapshot) {
				throw TransactionException("Table %s is already outdated. Please restart your transaction",
				                           GetTableKey());
			}

			auto &snapshot = snapshot_info.snapshot;
			D_ASSERT(snapshot);
			ret.table_metadata.SetCurrentSchemaId(table_metadata.GetCurrentSchemaId());
			ret.table_metadata.last_sequence_number = snapshot->sequence_number;
			ret.table_metadata.current_snapshot_id = snapshot->snapshot_id;
			return ret;
		}
		ret.table_metadata = ret.CreateMetadataFromLog(context, transaction_start_millis, ret.latest_metadata_json);
		return ret;
	}
	return ret;
}

void IcebergTableInformation::InitSchemaVersions() {
	for (auto &table_schema : table_metadata.GetSchemas()) {
		CreateSchemaVersion(*table_schema.second);
	}
}

IcebergTableInformation::IcebergTableInformation(IcebergCatalog &catalog, IcebergSchemaEntry &schema,
                                                 const string &name)
    : catalog(catalog), schema(schema), name(name) {
	table_id = "uuid-" + schema.name + "-" + name;
}

IcebergTransactionData &IcebergTableInformation::GetOrCreateTransactionData(IcebergTransaction &transaction) {
	lock_guard<mutex> guard(transaction.lock);
	if (!transaction_data) {
		auto context = transaction.context.lock();
		transaction_data = make_uniq<IcebergTransactionData>(*context, *this);
	}
	return *transaction_data;
}

void IcebergTableInformation::InitializeFromLoadTableResult(const rest_api_objects::LoadTableResult &load_table_result,
                                                            bool initialize_schemas) {
	table_metadata = IcebergTableMetadata::FromTableMetadata(load_table_result.metadata);
	config = load_table_result.config;
	storage_credentials.clear();
	for (auto &credential : load_table_result.storage_credentials) {
		storage_credentials.push_back(credential);
	}
	latest_metadata_json = load_table_result.metadata_location;

	if (initialize_schemas) {
		auto &schemas = table_metadata.GetSchemas();
		D_ASSERT(!schemas.empty());
		for (auto &table_schema : schemas) {
			CreateSchemaVersion(*table_schema.second);
		}
	}
}

} // namespace duckdb
