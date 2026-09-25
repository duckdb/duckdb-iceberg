#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/http_util.hpp"

#include "catalog/rest/api/url_utils.hpp"
#include "catalog/rest/iceberg_schema_set.hpp"
#include "rest_catalog/objects/load_table_result.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"
#include "common/iceberg_utils.hpp"

namespace duckdb {

class IcebergSchemaEntry;
struct IcebergTable;

class MetadataCacheValue {
public:
	MetadataCacheValue(timestamp_ms_t expire_timestamp_ms,
	                   unique_ptr<const rest_api_objects::LoadTableResult> load_table_result)
	    : expire_timestamp_ms(expire_timestamp_ms), load_table_result(std::move(load_table_result)) {
	}

public:
	//! The timestamp until when this entry is valid
	timestamp_ms_t expire_timestamp_ms;
	//! The payload of the cache entry
	unique_ptr<const rest_api_objects::LoadTableResult> load_table_result;
};

class LoadTableResultCache;

//! Caller-owned state controlling whether a fetched result can enter the cache.
//! The cache must outlive this state, which is kept separate from worker requests.
class LoadTableCachePublication {
public:
	~LoadTableCachePublication();
	bool TryPublish(unique_ptr<const rest_api_objects::LoadTableResult> result);

private:
	friend class LoadTableResultCache;
	explicit LoadTableCachePublication(string table_key) : table_key(std::move(table_key)) {
	}
	LoadTableCachePublication(const LoadTableCachePublication &) = delete;
	LoadTableCachePublication &operator=(const LoadTableCachePublication &) = delete;

	optional_ptr<LoadTableResultCache> cache;
	string table_key;
};

class LoadTableResultCache {
public:
	LoadTableResultCache(IcebergAttachOptions &attach_options) : attach_options(attach_options) {
	}

public:
	bool Get(ClientContext &context, const string &table_key,
	         const std::function<void(const rest_api_objects::LoadTableResult &)> &callback,
	         bool validate_cache = true) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto it = tables.find(table_key);
		if (it == tables.end()) {
			return false;
		}

		auto transaction_start_ms = IcebergUtils::GetTransactionStartTimeMS(context);

		auto &entry = it->second;
		if (validate_cache && transaction_start_ms > entry.expire_timestamp_ms) {
			// cached value has expired
			return false;
		}
		callback(*entry.load_table_result);
		return true;
	}
	//! Create publication state before fetching. A later fetch supersedes earlier loads for the same key.
	unique_ptr<LoadTableCachePublication> BeginLoad(const string &table_key);
	//! Authoritative publication (e.g. staged creation) also invalidates outstanding fetches.
	void SetOrOverwrite(const string &table_key, unique_ptr<const rest_api_objects::LoadTableResult> load_table_result);

	//! Evict only if the table was initialized from the result that is still cached for its key.
	void EvictIfCurrent(const IcebergTable &table);

private:
	friend class LoadTableCachePublication;
	struct PendingLoads {
		optional_ptr<LoadTableCachePublication> latest;
		idx_t count = 0;
	};
	bool TryPublish(LoadTableCachePublication &publication, unique_ptr<const rest_api_objects::LoadTableResult> result);
	void Release(LoadTableCachePublication &publication);
	void InvalidateLoads(const string &table_key) DUCKDB_REQUIRES(lock);
	void Store(const string &table_key, unique_ptr<const rest_api_objects::LoadTableResult> result)
	    DUCKDB_REQUIRES(lock);

	IcebergAttachOptions &attach_options;
	annotated_mutex lock;
	case_insensitive_map_t<MetadataCacheValue> tables DUCKDB_GUARDED_BY(lock);
	case_insensitive_map_t<PendingLoads> pending_loads DUCKDB_GUARDED_BY(lock);
};

class IcebergCatalog : public Catalog {
public:
	explicit IcebergCatalog(AttachedDatabase &db_p, AccessMode access_mode,
	                        unique_ptr<IcebergAuthorization> auth_handler, IcebergAttachOptions &attach_options,
	                        const optional<Identifier> &default_schema);
	~IcebergCatalog() override;

public:
	static unique_ptr<SecretEntry> GetStorageSecret(ClientContext &context, const string &secret_name);
	static unique_ptr<SecretEntry> GetIcebergSecret(ClientContext &context, const string &secret_name);
	static unique_ptr<SecretEntry> GetHTTPSecret(ClientContext &context, const string &secret_name);
	void ParsePrefix();
	void ParseNamespaceSeparator();
	void GetConfig(ClientContext &context, IcebergEndpointType &endpoint_type);
	IRCEndpointBuilder GetBaseUrl() const;
	string GetWarehouse() const {
		return warehouse;
	}
	//! Whether or not this catalog should search a specific type with the standard priority
	CatalogLookupBehavior CatalogTypeLookupRule(CatalogType type) const override {
		switch (type) {
		case CatalogType::TABLE_FUNCTION_ENTRY:
		case CatalogType::SCALAR_FUNCTION_ENTRY:
		case CatalogType::AGGREGATE_FUNCTION_ENTRY:
			return CatalogLookupBehavior::NEVER_LOOKUP;
		default:
			return CatalogLookupBehavior::STANDARD;
		}
	}
	bool CheckAmbiguousCatalogOrSchema(ClientContext &context, const Identifier &schema) override {
		return false;
	}
	optional<Identifier> GetDefaultSchema() const override;
	ErrorData SupportsCreateTable(BoundCreateTableInfo &info) override;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "iceberg";
	}
	bool SupportsTimeTravel() const override {
		return true;
	}
	void DropSchema(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	IcebergSchemaSet &GetSchemas();
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	//! Shared delete-planning body both PlanDelete and MERGE build on; only PlanDelete additionally opts the
	//! standalone DELETE into metadata-only deletes, so MERGE must plan its delete action through here directly.
	PhysicalOperator &PlanDeleteOperation(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                                      PhysicalOperator &plan);
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                PhysicalOperator &plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;
	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	void AddDefaultSupportedEndpoints();
	void AddS3TablesEndpoints();
	void AddGlueEndpoints();
	//! Whether or not this is an in-memory Iceberg database
	bool InMemory() override;
	string GetDBPath() override;
	//! Allow ATTACH OR REPLACE to actually re-attach when iceberg-specific options change
	bool HasConflictingAttachOptions(const string &path, const AttachOptions &options) override;
	void SetAttachOptions(const unordered_map<string, Value> &options);
	static string GetOnlyMergeOnReadSupportedErrorMessage(const string &table_name, const string &property,
	                                                      const string &property_value);

public:
	AccessMode access_mode;
	unique_ptr<IcebergAuthorization> auth_handler;
	//! Base URI of the REST catalog
	string base_uri;
	//! version
	const string version;
	//! optional prefix path components
	vector<string> prefix;
	string namespace_separator = "\x1f";
	//! attach options
	IcebergAttachOptions attach_options;
	//! Optionally (user-)provided default schema to use
	optional<Identifier> default_schema;

private:
	//! warehouse
	string warehouse;
	// defaults and overrides provided by a catalog.
	case_insensitive_map_t<string> defaults;
	case_insensitive_map_t<string> overrides;
	//! Normalized attach options (after core stripping) used to detect a conflicting ATTACH OR REPLACE
	unordered_map<string, Value> normalized_attach_options;

public:
	unordered_set<string> supported_urls;
	IcebergSchemaSet schemas;
	LoadTableResultCache table_request_cache;
};

} // namespace duckdb
