#pragma once

#include "storage/catalog/iceberg_table_entry.hpp"
#include "storage/iceberg_metadata_info.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_table_metadata.hpp"
#include "iceberg_transaction_data.hpp"
#include "duckdb/catalog/catalog_entry.hpp"

namespace duckdb {
class IcebergTableSchema;
struct CreateTableInfo;
class IcebergSchemaEntry;
struct IcebergManifestEntry;

struct IRCAPITableCredentials {
	unique_ptr<CreateSecretInput> config;
	vector<CreateSecretInput> storage_credentials;
};

struct IcebergTableInformation {
public:
	IcebergTableInformation(IcebergCatalog &catalog, IcebergSchemaEntry &schema, const string &name);

public:
	optional_ptr<CatalogEntry> GetLatestSchema();
	optional_ptr<CatalogEntry> GetSchemaVersion(optional_ptr<BoundAtClause> at);
	optional_ptr<CatalogEntry> CreateSchemaVersion(IcebergTableSchema &table_schema);
	IRCAPITableCredentials GetVendedCredentials(ClientContext &context);
	const string &BaseFilePath() const;

	void InitTransactionData(IcebergTransaction &transaction);
	void AddSnapshot(IcebergTransaction &transaction, vector<IcebergManifestEntry> &&data_files);
	void AddDeleteSnapshot(IcebergTransaction &transaction, vector<IcebergManifestEntry> &&data_files);
	void AddUpdateSnapshot(IcebergTransaction &transaction, vector<IcebergManifestEntry> &&delete_files,
	                       vector<IcebergManifestEntry> &&data_files);
	void AddSchema(IcebergTransaction &transaction);
	void AddAssertCreate(IcebergTransaction &transaction);
	void AddAssignUUID(IcebergTransaction &transaction);
	void AddUpradeFormatVersion(IcebergTransaction &transaction);
	void AddSetCurrentSchema(IcebergTransaction &transaction);
	void AddPartitionSpec(IcebergTransaction &transaction);
	void AddSortOrder(IcebergTransaction &transaction);
	void SetDefaultSortOrder(IcebergTransaction &transaction);
	void SetDefaultSpec(IcebergTransaction &transaction);
	void SetProperties(IcebergTransaction &transaction, case_insensitive_map_t<string> properties);
	void RemoveProperties(IcebergTransaction &transaction, vector<string> properties);
	void SetLocation(IcebergTransaction &transaction);
	bool IsTransactionLocalTable(IcebergTransaction &transaction);
	static string GetTableKey(const vector<string> &namespace_items, const string &table_name);
	string GetTableKey() const;
	// we pass the transaction, because we are only allowed to copy table information state provded by the catalog
	// from before our transaction start time.
	IcebergTableInformation Copy(IcebergTransaction &iceberg_transaction) const;
	// This copy is used for deletes, where we don't care about valid table state
	IcebergTableInformation Copy() const;
	void InitSchemaVersions();

	IcebergSnapshotLookup GetSnapshotLookup(IcebergTransaction &iceberg_transaction) const;
	IcebergSnapshotLookup GetSnapshotLookup(ClientContext &context) const;
	bool TableIsEmpty(const IcebergSnapshotLookup &snapshot_lookup) const;
	bool HasTransactionUpdates();

public:
	IcebergCatalog &catalog;
	IcebergSchemaEntry &schema;
	string name;
	string table_id;
	IcebergTableMetadata table_metadata;

	unordered_map<int32_t, unique_ptr<IcebergTableEntry>> schema_versions;
	// dummy entry to hold existence of a table, but no schema versions
	unique_ptr<IcebergTableEntry> dummy_entry;
	unique_ptr<IcebergTransactionData> transaction_data;
};

} // namespace duckdb
