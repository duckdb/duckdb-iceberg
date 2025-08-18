#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "storage/irc_table_entry.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_table_metadata.hpp"

namespace duckdb {
class IcebergTableSchema;
struct CreateTableInfo;
class IRCSchemaEntry;
struct IcebergManifestEntry;

struct IRCAPITableCredentials {
	unique_ptr<CreateSecretInput> config;
	vector<CreateSecretInput> storage_credentials;
};

struct IcebergTableInformation {
public:
	IcebergTableInformation(IRCatalog &catalog, IRCSchemaEntry &schema, const string &name);

public:
	optional_ptr<CatalogEntry> GetSchemaVersion(optional_ptr<BoundAtClause> at);
	optional_ptr<CatalogEntry> CreateSchemaVersion(IcebergTableSchema &table_schema);
	IRCAPITableCredentials GetVendedCredentials(ClientContext &context);
	const string &BaseFilePath() const;

	void InitTransactionData(IRCTransaction &transaction);
	void AddSnapshot(IRCTransaction &transaction, vector<IcebergManifestEntry> &&data_files);
	void AddDeleteSnapshot(IRCTransaction &transaction, vector<IcebergManifestEntry> &&data_files);
	void AddSchema(IRCTransaction &transaction);
	void AddAssertCreate(IRCTransaction &transaction);
	void AddAssignUUID(IRCTransaction &transaction);
	void AddUpradeFormatVersion(IRCTransaction &transaction);
	void AddSetCurrentSchema(IRCTransaction &transaction);
	void AddPartitionSpec(IRCTransaction &transaction);
	void AddSortOrder(IRCTransaction &transaction);
	void SetDefaultSortOrder(IRCTransaction &transaction);
	void SetDefaultSpec(IRCTransaction &transaction);
	void SetProperties(IRCTransaction &transaction, case_insensitive_map_t<string> properties);
	void SetLocation(IRCTransaction &transaction);

public:
	IRCatalog &catalog;
	IRCSchemaEntry &schema;
	string name;
	string table_id;
	// bool deleted;

	rest_api_objects::LoadTableResult load_table_result;
	IcebergTableMetadata table_metadata;
	unordered_map<int32_t, unique_ptr<ICTableEntry>> schema_versions;

public:
	unique_ptr<IcebergTransactionData> transaction_data;
};
} // namespace duckdb
