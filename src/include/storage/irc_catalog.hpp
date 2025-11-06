#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "url_utils.hpp"
#include "storage/irc_schema_set.hpp"
#include "rest_catalog/objects/load_table_result.hpp"
#include "storage/irc_authorization.hpp"

#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/http_util.hpp"

namespace duckdb {

class IRCSchemaEntry;

class MetadataCacheValue {
public:
	const string data;
	const system_clock::time_point expires_at;

public:
	MetadataCacheValue(const string &data_, const system_clock::time_point expires_at_)
	    : data(data_), expires_at(expires_at_) {
	}
};

class IRCatalog : public Catalog {
public:
	// default target file size: 8.4MB
	static constexpr const idx_t DEFAULT_TARGET_FILE_SIZE = 1 << 23;

public:
	explicit IRCatalog(AttachedDatabase &db_p, AccessMode access_mode, unique_ptr<IRCAuthorization> auth_handler,
	                   IcebergAttachOptions &attach_options, const string &default_schema);
	~IRCatalog() override;

public:
	static unique_ptr<SecretEntry> GetStorageSecret(ClientContext &context, const string &secret_name);
	static unique_ptr<SecretEntry> GetIcebergSecret(ClientContext &context, const string &secret_name);
	void GetConfig(ClientContext &context, IcebergEndpointType &endpoint_type);
	IRCEndpointBuilder GetBaseUrl() const;
	string OptionalGetCachedValue(const string &url);
	bool SetCachedValue(const string &url, const string &value, const rest_api_objects::LoadTableResult &result);
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
	//! Whether or not this is an in-memory Iceberg database
	bool InMemory() override;
	string GetDBPath() override;
	string GetURLEncodedPrefix();

	static string GetOnlyMergeOnReadSupportedErrorMessage(const string &table_name, const string &property,
	                                                      const string &property_value);

public:
	AccessMode access_mode;
	unique_ptr<IRCAuthorization> auth_handler;
	IRCEndpointBuilder endpoint_builder;
	//! warehouse
	string warehouse;
	//! host of the REST catalog
	string uri;
	//! version
	const string version;
	//! optional prefix
	string prefix;
	//! attach options
	IcebergAttachOptions attach_options;
	string default_schema;

private:
	// defaults and overrides provided by a catalog.
	case_insensitive_map_t<string> defaults;
	case_insensitive_map_t<string> overrides;

public:
	unordered_set<string> supported_urls;

private:
	std::mutex metadata_cache_mutex;
	unordered_map<string, unique_ptr<MetadataCacheValue>> metadata_cache;
};

} // namespace duckdb
