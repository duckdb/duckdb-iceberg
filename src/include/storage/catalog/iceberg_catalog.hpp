#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "url_utils.hpp"
#include "storage/catalog/iceberg_schema_set.hpp"
#include "rest_catalog/objects/load_table_result.hpp"
#include "storage/iceberg_authorization.hpp"

#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/http_util.hpp"

namespace duckdb {

class IcebergSchemaEntry;

class MetadataCacheValue {
public:
	const system_clock::time_point expires_at;
	unique_ptr<const rest_api_objects::LoadTableResult> load_table_result;

public:
	MetadataCacheValue(const system_clock::time_point expires_at,
	                   unique_ptr<const rest_api_objects::LoadTableResult> load_table_result)
	    : expires_at(expires_at), load_table_result(std::move(load_table_result)) {
	}
};

class IcebergCatalog : public Catalog {
public:
	// default target file size: 8.4MB
	static constexpr const idx_t DEFAULT_TARGET_FILE_SIZE = 1 << 23;

public:
	explicit IcebergCatalog(AttachedDatabase &db_p, AccessMode access_mode,
	                        unique_ptr<IcebergAuthorization> auth_handler, IcebergAttachOptions &attach_options,
	                        const string &default_schema);
	~IcebergCatalog() override;

public:
	static unique_ptr<SecretEntry> GetStorageSecret(ClientContext &context, const string &secret_name);
	static unique_ptr<SecretEntry> GetIcebergSecret(ClientContext &context, const string &secret_name);
	void ParsePrefix();
	void GetConfig(ClientContext &context, IcebergEndpointType &endpoint_type);
	IRCEndpointBuilder GetBaseUrl() const;
	static void SetAWSCatalogOptions(IcebergAttachOptions &attach_options,
	                                 case_insensitive_set_t &set_by_attach_options);
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
	bool CheckAmbiguousCatalogOrSchema(ClientContext &context, const string &schema) override {
		return false;
	}
	string GetDefaultSchema() const override {
		return default_schema;
	}

public:
	static unique_ptr<Catalog> Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
	                                  AttachedDatabase &db, const string &name, AttachInfo &info,
	                                  AttachOptions &options);

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
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
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
	static string GetOnlyMergeOnReadSupportedErrorMessage(const string &table_name, const string &property,
	                                                      const string &property_value);
	void StoreLoadTableResult(const string &table_key,
	                          unique_ptr<const rest_api_objects::LoadTableResult> load_table_result);
	MetadataCacheValue &GetLoadTableResult(const string &table_key);
	//! Returns a reference to the metadata cache mutex. The caller is responsible for holding the lock
	//! for the duration of any access to data returned by TryGetValidCachedLoadTableResult.
	std::mutex &GetMetadataCacheLock();
	optional_ptr<MetadataCacheValue> TryGetValidCachedLoadTableResult(const string &table_key,
	                                                                  lock_guard<std::mutex> &lock);
	void RemoveLoadTableResult(const string &table_key);

public:
	AccessMode access_mode;
	unique_ptr<IcebergAuthorization> auth_handler;
	//! warehouse
	string warehouse;
	//! host of the REST catalog
	string uri;
	//! version
	const string version;
	//! optional prefix
	string prefix;
	bool prefix_is_one_component = true;
	//! attach options
	IcebergAttachOptions attach_options;
	string default_schema;

private:
	// defaults and overrides provided by a catalog.
	case_insensitive_map_t<string> defaults;
	case_insensitive_map_t<string> overrides;

public:
	unordered_set<string> supported_urls;
	IcebergSchemaSet schemas;

private:
	std::mutex metadata_cache_mutex;
	case_insensitive_map_t<unique_ptr<MetadataCacheValue>> metadata_cache;
};

} // namespace duckdb
