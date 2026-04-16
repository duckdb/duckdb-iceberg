#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include "catalog/rest/api/url_utils.hpp"
#include "catalog/rest/iceberg_schema_set.hpp"
#include "rest_catalog/objects/load_table_result.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"

namespace duckdb {

class IcebergSchemaEntry;

class MetadataCacheValue {
public:
	MetadataCacheValue(transaction_t creator, const system_clock::time_point expires_at,
	                   unique_ptr<const rest_api_objects::LoadTableResult> load_table_result)
	    : creator(creator), expires_at(expires_at), load_table_result(std::move(load_table_result)) {
	}

public:
	//! Store the id of the transaction that added the entry
	transaction_t creator;
	//! The timestamp until when this entry is valid
	const system_clock::time_point expires_at;
	//! The payload of the cache entry
	unique_ptr<const rest_api_objects::LoadTableResult> load_table_result;
};

class LoadTableResultCache {
public:
	LoadTableResultCache() {
	}

public:
	mutex &Lock() {
		return lock;
	}

	//! NOTE: lock needs to be held by the caller until the result goes out of scope
	optional_ptr<MetadataCacheValue> Get(const string &table_key, lock_guard<mutex> &lock, bool validate_cache = true) {
		(void)lock;
		auto it = tables.find(table_key);
		if (it == tables.end()) {
			return nullptr;
		}
		auto &entry = it->second;
		if (validate_cache && system_clock::now() > entry.expires_at) {
			// cached value has expired
			return nullptr;
		}
		return entry;
	}
	void SetOrOverwrite(ClientContext &context, const string &table_key,
	                    unique_ptr<const rest_api_objects::LoadTableResult> load_table_result) {
		lock_guard<mutex> guard(lock);
		// erase load table result if it exists.
		tables.erase(table_key);

		// If max_table_staleness_minutes is not set, use a time in the past so cache is always expired
		system_clock::time_point expires_at;
		if (max_table_staleness_micros.IsValid()) {
			expires_at = system_clock::now() + std::chrono::microseconds(max_table_staleness_micros.GetIndex());
		} else {
			expires_at = system_clock::time_point::min();
		}
		auto &meta_transaction = MetaTransaction::Get(context);
		tables.emplace(table_key, MetadataCacheValue(meta_transaction.global_transaction_id, expires_at,
		                                             std::move(load_table_result)));
	}
	void Expire(ClientContext &context, const string &table_key) {
		auto &meta_transaction = MetaTransaction::Get(context);
		lock_guard<mutex> guard(lock);
		auto it = tables.find(table_key);
		if (it == tables.end()) {
			//! Entry doesn't exist anymore
			return;
		}
		auto &entry = it->second;
		if (entry.creator != meta_transaction.global_transaction_id) {
			//! The entry we made is no longer the latest version, can't expire
			return;
		}
		tables.erase(it);
	}
	void SetMaxTableStaleness(optional_idx micros) {
		max_table_staleness_micros = micros;
	}

private:
	optional_idx max_table_staleness_micros;
	mutex lock;
	case_insensitive_map_t<MetadataCacheValue> tables;
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
	static unique_ptr<SecretEntry> GetHTTPSecret(ClientContext &context, const string &secret_name);
	void ParsePrefix();
	void GetConfig(ClientContext &context, IcebergEndpointType &endpoint_type);
	IRCEndpointBuilder GetBaseUrl() const;
	string GetWarehouse() const {
		return warehouse;
	}
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
	ErrorData SupportsCreateTable(BoundCreateTableInfo &info) override;

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

public:
	AccessMode access_mode;
	unique_ptr<IcebergAuthorization> auth_handler;
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
	//! warehouse
	string warehouse;
	// defaults and overrides provided by a catalog.
	case_insensitive_map_t<string> defaults;
	case_insensitive_map_t<string> overrides;

public:
	unordered_set<string> supported_urls;
	IcebergSchemaSet schemas;
	LoadTableResultCache table_request_cache;
};

} // namespace duckdb
