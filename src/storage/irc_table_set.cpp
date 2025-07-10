#include "catalog_api.hpp"
#include "catalog_utils.hpp"

#include "storage/irc_catalog.hpp"
#include "storage/irc_table_set.hpp"

#include "storage/irc_transaction.hpp"
#include "metadata/iceberg_partition_spec.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/irc_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "storage/irc_transaction.hpp"

#include "storage/authorization/sigv4.hpp"
#include "storage/authorization/oauth2.hpp"

namespace duckdb {

IcebergTableInformation::IcebergTableInformation(IRCatalog &catalog, IRCSchemaEntry &schema, const string &name)
    : catalog(catalog), schema(schema), name(name) {
	table_id = "uuid-" + schema.name + "-" + name;
}

void IcebergTableInformation::AddSnapshot(IRCTransaction &transaction, vector<IcebergManifestEntry> &&data_files) {
	if (!transaction_data) {
		auto context = transaction.context.lock();
		transaction_data = make_uniq<IcebergTransactionData>(*context, *this);
	}

	transaction_data->AddSnapshot(IcebergSnapshotOperationType::APPEND, std::move(data_files));
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
		endpoint = endpoint.substr(7, string::npos);
	}
	if (StringUtil::StartsWith(endpoint, "https://")) {
		endpoint = endpoint.substr(8, string::npos);
	}
	if (StringUtil::EndsWith(endpoint, "/")) {
		endpoint = endpoint.substr(0, endpoint.size() - 1);
	}
	endpoint_it->second = endpoint;
}

const string &IcebergTableInformation::BaseFilePath() const {
	return load_table_result.metadata.location;
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

	// Mapping from config key to a duckdb secret option

	case_insensitive_map_t<Value> config_options;
	//! TODO: apply the 'defaults' retrieved from the /v1/config endpoint
	config_options.insert(user_defaults.begin(), user_defaults.end());

	if (load_table_result.has_config) {
		auto &config = load_table_result.config;
		ParseConfigOptions(config, config_options);
	}

	const auto &metadata_location = load_table_result.metadata.location;

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

ICTableSet::ICTableSet(IRCSchemaEntry &schema) : schema(schema), catalog(schema.ParentCatalog()) {
}

void ICTableSet::FillEntry(ClientContext &context, IcebergTableInformation &table) {
	if (!table.schema_versions.empty()) {
		//! Already filled
		return;
	}

	auto &ic_catalog = catalog.Cast<IRCatalog>();
	table.load_table_result = IRCAPI::GetTable(context, ic_catalog, schema.name, table.name);
	table.table_metadata = IcebergTableMetadata::FromTableMetadata(table.load_table_result.metadata);
	auto &schemas = table.table_metadata.schemas;

	//! It should be impossible to have a metadata file without any schema
	D_ASSERT(!schemas.empty());
	for (auto &table_schema : schemas) {
		table.CreateSchemaVersion(*table_schema.second);
	}
}

void ICTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> l(entry_lock);
	LoadEntries(context);
	for (auto &entry : entries) {
		auto &table_info = entry.second;
		FillEntry(context, table_info);
		auto schema_id = table_info.table_metadata.current_schema_id;
		callback(*table_info.schema_versions[schema_id]);
	}
}

void ICTableSet::LoadEntries(ClientContext &context) {
	if (!entries.empty()) {
		return;
	}

	auto &ic_catalog = catalog.Cast<IRCatalog>();
	// TODO: handle out-of-order columns using position property
	auto tables = IRCAPI::GetTables(context, ic_catalog, schema.name);

	for (auto &table : tables) {
		entries.emplace(table.name, IcebergTableInformation(ic_catalog, schema, table.name));
	}
}

void ICTableSet::CreateNewEntry(ClientContext &context, IRCatalog &catalog, IRCSchemaEntry &schema,
                                CreateTableInfo &info) {
	auto table_name = info.table;
	if (entries.find(table_name) != entries.end()) {
		throw CatalogException("Table %s already exists", table_name.c_str());
	}

	entries.emplace(table_name, IcebergTableInformation(catalog, schema, info.table));
	auto &table_info = entries.find(table_name)->second;

	auto table_entry = make_uniq<ICTableEntry>(table_info, catalog, schema, info);
	auto optional_entry = table_entry.get();

	optional_entry->table_info.schema_versions[0] = std::move(table_entry);
	optional_entry->table_info.table_metadata.schemas[0] =
	    IcebergCreateTableRequest::CreateIcebergSchema(optional_entry);
	optional_entry->table_info.table_metadata.current_schema_id = 0;
	optional_entry->table_info.table_metadata.schemas[0]->schema_id = 0;
	auto &irc_transaction = IRCTransaction::Get(context, catalog);
	// Immediately create the table with stage_create = true to get metadata & data location(s)
	// transaction commit will either commit with data (OR) create the table with stage_create = false
	// on abort, hit DELETE endpoint with purge = TRUE?
	auto load_table_result = irc_transaction.CommitNewTable(context, optional_entry, true);
	optional_entry->table_info.load_table_result = std::move(load_table_result);
	optional_entry->table_info.table_metadata =
	    IcebergTableMetadata::FromTableMetadata(optional_entry->table_info.load_table_result.metadata);
}

unique_ptr<ICTableInfo> ICTableSet::GetTableInfo(ClientContext &context, IRCSchemaEntry &schema,
                                                 const string &table_name) {
	throw NotImplementedException("ICTableSet::GetTableInfo");
}

optional_ptr<CatalogEntry> ICTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	LoadEntries(context);
	lock_guard<mutex> l(entry_lock);
	auto entry = entries.find(lookup.GetEntryName());
	if (entry == entries.end()) {
		return nullptr;
	}
	FillEntry(context, entry->second);
	return entry->second.GetSchemaVersion(lookup.GetAtClause());
}

} // namespace duckdb
