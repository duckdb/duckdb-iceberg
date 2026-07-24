#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system_wrapper.hpp"

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

struct IcebergTableInformation {
public:
	IcebergTableInformation(IcebergCatalog &catalog, IcebergSchemaEntry &schema, const string &name);

public:
	void LoadCredentials(ClientContext &context) const;
	void LoadCredentials(ClientContext &context, IRCAPITableCredentials table_credentials) const;
	optional_ptr<CatalogEntry> GetLatestSchema();
	idx_t GetIcebergVersion() const;
	optional_ptr<CatalogEntry> GetSchemaVersion(optional_ptr<BoundAtClause> at);
	optional_ptr<CatalogEntry> CreateSchemaVersion(const IcebergTableSchema &table_schema);
	idx_t GetMaxSchemaId();
	idx_t GetNextPartitionSpecId();
	idx_t GetNextSortOrderId();
	optional<int64_t> GetExistingSpecId(IcebergPartitionSpec &spec);
	optional<int64_t> GetExistingSortOrderId(IcebergSortOrder &sort_order);
	void SetPartitionedBy(IcebergTransaction &transaction, const vector<unique_ptr<ParsedExpression>> &partition_keys,
	                      const IcebergTableSchema &schema);
	void SetSortedBy(IcebergTransaction &transaction, const vector<OrderByNode> &orders,
	                 const IcebergTableSchema &schema, bool first_sort_spec = false);
	//! Build an IcebergPartitionSpec from parsed PARTITIONED BY expressions and a schema.
	static IcebergPartitionSpec BuildPartitionSpec(const vector<unique_ptr<ParsedExpression>> &partition_keys,
	                                               const IcebergTableSchema &schema, int32_t spec_id,
	                                               idx_t base_partition_field_id);
	static IcebergSortOrder BuildSortOrder(const vector<OrderByNode> &orders, const IcebergTableSchema &schema,
	                                       int32_t sort_order_id);
	IRCAPITableCredentials GetVendedCredentials(ClientContext &context) const;
	IRCAPITableCredentials
	GetVendedCredentials(ClientContext &context,
	                     const vector<rest_api_objects::StorageCredential> &storage_credentials) const;
	const string &BaseFilePath() const;

	IcebergTransactionData &GetOrCreateTransactionData(IcebergTransaction &transaction);

	static string GetTableKey(const vector<string> &namespace_items, const string &table_name);
	string GetTableKey() const;
	IcebergTableMetadata CreateMetadataFromLog(ClientContext &context, timestamp_ms_t transaction_start_ms) const;
	// With metadata-log enabled, reconstruct the complete table state at transaction start. Otherwise pin and copy
	// the complete catalog state that was resolved for this transaction.
	IcebergTableInformation Copy(IcebergTransaction &iceberg_transaction) const;
	// This copy is used for deletes, where we don't care about valid table state
	IcebergTableInformation Copy() const;
	void InitSchemaVersions();

	bool HasTransactionUpdates() const;
	void InitializeFromLoadTableResult(const rest_api_objects::LoadTableResult &load_table_result,
	                                   bool initialize_schemas = true);
	void RefreshFromCatalog(ClientContext &context);

public:
	IcebergCatalog &catalog;
	IcebergSchemaEntry &schema;
	string name;
	string table_id;
	IcebergTableMetadata table_metadata;
	case_insensitive_map_t<string> config;
	vector<rest_api_objects::StorageCredential> storage_credentials;
	unordered_map<int32_t, unique_ptr<IcebergTableEntry>> schema_versions;
	// dummy entry to hold existence of a table, but no schema versions
	unique_ptr<IcebergTableEntry> dummy_entry;
	unique_ptr<IcebergTransactionData> transaction_data;
};

} // namespace duckdb
