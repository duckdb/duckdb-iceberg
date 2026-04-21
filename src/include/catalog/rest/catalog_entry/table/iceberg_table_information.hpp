#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/storage/caching_file_system_wrapper.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "rest_catalog/objects/storage_credential.hpp"

namespace duckdb {
class IcebergTableSchema;
class ParsedExpression;
struct CreateTableInfo;
class IcebergSchemaEntry;
struct IcebergManifestEntry;

struct IRCAPITableCredentials {
	unique_ptr<CreateSecretInput> config;
	vector<CreateSecretInput> storage_credentials;
};

struct IcebergTableStorageCredential {
public:
	IcebergTableStorageCredential(const rest_api_objects::StorageCredential &storage_credential) {
		prefix = storage_credential.prefix;
		config = storage_credential.config;
	}

public:
	string prefix;
	case_insensitive_map_t<string> config;
};

struct IcebergTableInformation {
public:
	IcebergTableInformation(IcebergCatalog &catalog, IcebergSchemaEntry &schema, const string &name);

public:
	optional_ptr<CatalogEntry> GetLatestSchema(ClientContext &context);
	idx_t GetIcebergVersion() const;
	optional_ptr<CatalogEntry> GetSchemaVersion(ClientContext &context, optional_ptr<BoundAtClause> at);
	optional_ptr<CatalogEntry> CreateSchemaVersion(const IcebergTableSchema &table_schema);
	idx_t GetMaxSchemaId();
	idx_t GetNextPartitionSpecId();
	int64_t GetExistingSpecId(IcebergPartitionSpec &spec);
	void SetPartitionedBy(IcebergTransaction &transaction, const vector<unique_ptr<ParsedExpression>> &partition_keys,
	                      const IcebergTableSchema &schema, bool first_partition_spec = false);
	IRCAPITableCredentials GetVendedCredentials(ClientContext &context);
	const string &BaseFilePath() const;

	IcebergTransactionData &GetOrCreateTransactionData(IcebergTransaction &transaction);

	static string GetTableKey(const vector<string> &namespace_items, const string &table_name);
	string GetTableKey() const;
	IcebergTableMetadata CreateMetadataFromLog(ClientContext &context, int64_t transaction_start_millis,
	                                           string &metadata_path) const;
	// we pass the transaction, because we are only allowed to copy table information state provded by the catalog
	// from before our transaction start time.
	IcebergTableInformation Copy(IcebergTransaction &iceberg_transaction) const;
	// This copy is used for deletes, where we don't care about valid table state
	IcebergTableInformation Copy(ClientContext &context) const;
	void InitSchemaVersions();

	IcebergSnapshotLookup GetSnapshotLookup(IcebergTransaction &iceberg_transaction) const;
	IcebergSnapshotLookup GetSnapshotLookup(ClientContext &context, optional_ptr<BoundAtClause> at) const;
	IcebergSnapshotLookup GetSnapshotLookup(ClientContext &context) const;
	bool TableIsEmpty(const IcebergSnapshotLookup &snapshot_lookup) const;
	bool HasTransactionUpdates() const;
	void InitializeFromLoadTableResult(const rest_api_objects::LoadTableResult &load_table_result,
	                                   bool initialize_schemas = true);

public:
	IcebergCatalog &catalog;
	IcebergSchemaEntry &schema;
	string name;
	string table_id;
	IcebergTableMetadata table_metadata;
	case_insensitive_map_t<string> config;
	vector<IcebergTableStorageCredential> storage_credentials;
	// when loading table metadata, store the path to the metadata.json for extension functions like iceberg_metadata()
	string latest_metadata_json;

	unordered_map<int32_t, unique_ptr<IcebergTableEntry>> schema_versions;
	// dummy entry to hold existence of a table, but no schema versions
	unique_ptr<IcebergTableEntry> dummy_entry;
	unique_ptr<IcebergTransactionData> transaction_data;
};

} // namespace duckdb
