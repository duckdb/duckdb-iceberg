#include "storage/catalog/iceberg_catalog.hpp"
#include "storage/catalog/iceberg_schema_entry.hpp"
#include "storage/catalog/iceberg_table_entry.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "catalog_api.hpp"
#include "iceberg_multi_file_reader.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "storage/authorization/sigv4.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "iceberg_multi_file_reader.hpp"

#include "rest_catalog/objects/list.hpp"
#include "storage/iceberg_table_information.hpp"

namespace duckdb {
class OAuth2Authorization;
constexpr column_t IcebergMultiFileReader::COLUMN_IDENTIFIER_LAST_SEQUENCE_NUMBER;

IcebergTableEntry::IcebergTableEntry(IcebergTableInformation &table_info, Catalog &catalog, SchemaCatalogEntry &schema,
                                     CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info), table_info(table_info) {
	this->internal = false;
}

unique_ptr<BaseStatistics> IcebergTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void AddHTTPSecretsToOptions(SecretEntry &http_secret_entry, case_insensitive_map_t<Value> &options) {
	auto http_kv_secret = dynamic_cast<const KeyValueSecret &>(*http_secret_entry.secret);

	options["http_proxy"] =
	    http_kv_secret.TryGetValue("http_proxy").IsNull() ? "" : http_kv_secret.TryGetValue("http_proxy").ToString();
	options["verify_ssl"] = http_kv_secret.TryGetValue("verify_ssl").IsNull()
	                            ? Value::BOOLEAN(true)
	                            : http_kv_secret.TryGetValue("verify_ssl").DefaultCastAs(LogicalType::BOOLEAN);
}

void IcebergTableEntry::PrepareIcebergScanFromEntry(ClientContext &context) const {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &secret_manager = SecretManager::Get(context);

	if (ic_catalog.attach_options.access_mode != IRCAccessDelegationMode::VENDED_CREDENTIALS) {
		// assume secret already exists
		return;
	}
	// Get Credentials from IRC API
	auto table_credentials = table_info.GetVendedCredentials(context);
	auto metadata_path = table_info.table_metadata.GetMetadataPath();

	unique_ptr<SecretEntry> http_secret_entry;
	unique_ptr<SIGV4Authorization> sigv4_auth;

	switch (ic_catalog.auth_handler->type) {
	case IcebergAuthorizationType::SIGV4: {
		auto &sigv4 = ic_catalog.auth_handler->Cast<SIGV4Authorization>();
		sigv4_auth = make_uniq<SIGV4Authorization>(sigv4.secret);

		http_secret_entry = IcebergCatalog::GetHTTPSecret(context, sigv4_auth->secret);
		break;
	}
	case IcebergAuthorizationType::OAUTH2: {
		http_secret_entry = IcebergCatalog::GetHTTPSecret(context, "");

		if (!http_secret_entry || http_secret_entry->secret->GetScope().size() == 0) {
			break;
		}
		for (auto scope : http_secret_entry->secret->GetScope()) {
			if (scope.find(ic_catalog.GetBaseUrl().GetHost()) != string::npos) {
				break;
			}
		}
	}
	default:
		break;
	}

	if (table_credentials.config) {
		auto &info = *table_credentials.config;
		D_ASSERT(info.scope.empty());
		string lc_storage_location = StringUtil::Lower(metadata_path);
		size_t metadata_pos = lc_storage_location.find("metadata");
		if (metadata_pos != string::npos) {
			info.scope = {metadata_path.substr(0, metadata_pos)};
		} else {
			DUCKDB_LOG_INFO(context, "Creating Iceberg Table secret with no scope. Returned metadata location is %s",
			                lc_storage_location);
		}

		if (StringUtil::StartsWith(ic_catalog.uri, "glue")) {
			D_ASSERT(sigv4_auth);
			auto secret_entry = IcebergCatalog::GetStorageSecret(context, sigv4_auth->secret);
			auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

			//! Override the endpoint if 'glue' is the host of the catalog
			auto region = kv_secret.TryGetValue("region").ToString();
			auto endpoint = "s3." + region + ".amazonaws.com";
			info.options["endpoint"] = endpoint;
		} else if (StringUtil::StartsWith(ic_catalog.uri, "s3tables")) {
			D_ASSERT(sigv4_auth);
			auto secret_entry = IcebergCatalog::GetStorageSecret(context, sigv4_auth->secret);
			auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

			//! Override all the options if 's3tables' is the host of the catalog
			auto substrings = StringUtil::Split(ic_catalog.warehouse, ":");
			D_ASSERT(substrings.size() == 6);
			auto region = substrings[3];
			auto endpoint = "s3." + region + ".amazonaws.com";

			info.options = {{"key_id", kv_secret.TryGetValue("key_id").ToString()},
			                {"secret", kv_secret.TryGetValue("secret").ToString()},
			                {"session_token", kv_secret.TryGetValue("session_token").IsNull()
			                                      ? ""
			                                      : kv_secret.TryGetValue("session_token").ToString()},
			                {"region", region},
			                {"endpoint", endpoint}};
		}

		if (http_secret_entry) {
			AddHTTPSecretsToOptions(*http_secret_entry, info.options);
		}

		(void)secret_manager.CreateSecret(context, info);
		// if there is no key_id, secret, or token in the info. log that vended credentials has not worked
		if (info.options.find("key_id") == info.options.end() && info.options.find("secret") == info.options.end() &&
		    info.options.find("token") == info.options.end()) {
			DUCKDB_LOG_INFO(context, "Failed to create valid secret from Vendend Credentials for table '%s'",
			                table_info.name);
		}
	} else {
		for (auto &info : table_credentials.storage_credentials) {
			if (http_secret_entry) {
				AddHTTPSecretsToOptions(*http_secret_entry, info.options);
			}
			(void)secret_manager.CreateSecret(context, info);
		}
	}
}

TableFunction IcebergTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                 const EntryLookupInfo &lookup) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &catalog_schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto catalog_entry = catalog_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "iceberg_scan");
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"iceberg_scan\" not found!");
	}
	auto &iceberg_scan_function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	auto iceberg_scan_function =
	    iceberg_scan_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});
	PrepareIcebergScanFromEntry(context);
	auto storage_location = table_info.table_metadata.location;

	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	// lookup should be asof start of the transaction if the lookup info is empty and there are no transaction updates
	bool using_transaction_timestamp = false;
	IcebergSnapshotLookup snapshot_lookup;
	if (!lookup.GetAtClause() && !table_info.HasTransactionUpdates()) {
		// if there is no user supplied AT () clause, and the table does not have transaction updates
		// use transaction start time
		snapshot_lookup = table_info.GetSnapshotLookup(context);
		using_transaction_timestamp = true;
	} else {
		auto at = lookup.GetAtClause();
		snapshot_lookup = IcebergSnapshotLookup::FromAtClause(at);
	}
	auto &metadata = table_info.table_metadata;
	optional_ptr<const IcebergSnapshot> snapshot = nullptr;
	try {
		snapshot = metadata.GetSnapshot(snapshot_lookup);
	} catch (InvalidConfigurationException &e) {
		if (!table_info.TableIsEmpty(snapshot_lookup)) {
			if (using_transaction_timestamp) {
				// We are using the transaction start time.
				// The table is not empty, but GetSnapshot is asking for table state before the first snapshot
				// table creation has no snapshot, so we return this error message
				throw InvalidConfigurationException("Table %s does not have a reachable state in this transaction",
				                                    table_info.GetTableKey());
			}
			throw e;
		}
		// try without transaction start time bounds. This is allowed to throw
		snapshot_lookup = IcebergSnapshotLookup::FromAtClause(lookup.GetAtClause());
		snapshot = metadata.GetSnapshot(snapshot_lookup);
	}

	int32_t schema_id;
	if (snapshot_lookup.IsLatest()) {
		schema_id = metadata.current_schema_id;
	} else {
		D_ASSERT(snapshot);
		schema_id = snapshot->schema_id;
	}

	auto iceberg_schema = metadata.GetSchemaFromId(schema_id);
	auto scan_info = make_shared_ptr<IcebergScanInfo>(metadata.GetMetadataPath(), metadata, snapshot, *iceberg_schema);
	if (table_info.transaction_data && snapshot_lookup.IsLatest()) {
		scan_info->transaction_data = table_info.transaction_data.get();
	}

	iceberg_scan_function.function_info = scan_info;

	// Set the S3 path as input to table function
	vector<Value> inputs = {storage_location};
	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, iceberg_scan_function,
	                                  empty_ref);
	auto result = iceberg_scan_function.bind(context, bind_input, return_types, names);
	bind_data = std::move(result);
	auto &file_bind_data = bind_data->Cast<MultiFileBindData>();
	D_ASSERT(file_bind_data.file_list);
	auto &ic_file_list = file_bind_data.file_list->Cast<IcebergMultiFileList>();
	ic_file_list.table = this;
	return iceberg_scan_function;
}

TableFunction IcebergTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	throw InternalException("IcebergTableEntry::GetScanFunction called without entry lookup info");
}

virtual_column_map_t IcebergTableEntry::GetVirtualColumns() const {
	return VirtualColumns();
}

virtual_column_map_t IcebergTableEntry::VirtualColumns() {
	virtual_column_map_t result;
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR));
	result.emplace(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::BIGINT));
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	               TableColumn("file_row_number", LogicalType::BIGINT));
	result.emplace(IcebergMultiFileReader::COLUMN_IDENTIFIER_LAST_SEQUENCE_NUMBER,
	               TableColumn("sequence_number", LogicalType::BIGINT));
	return result;
}

vector<column_t> IcebergTableEntry::GetRowIdColumns() const {
	vector<column_t> result;
	result.push_back(COLUMN_IDENTIFIER_ROW_ID);
	result.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILENAME);
	result.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
	return result;
}

TableStorageInfo IcebergTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	// TODO fill info
	return result;
}

string IcebergTableEntry::GetUUID() const {
	return table_info.table_id;
}

} // namespace duckdb
