
#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "url_utils.hpp"
#include "storage/irc_schema_set.hpp"

namespace duckdb {

class IRCSchemaEntry;

struct IRCCredentials {
	string key_id;
	string secret;
	// required to query s3 tables
	string region;
	// Catalog generates the token using client id & secret
	string token;
};

class ICRClearCacheFunction : public TableFunction {
public:
	ICRClearCacheFunction();

	static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
};

enum class ICEBERG_CATALOG_TYPE { AWS_S3TABLES, AWS_GLUE, OTHER, INVALID };

class MetadataCacheValue {
public:
	std::string data;
	std::chrono::system_clock::time_point expires_at;

public:
	MetadataCacheValue(std::string data_, std::chrono::system_clock::time_point expires_at_)
	    : data(data_), expires_at(expires_at_) {};
};

class IRCatalog : public Catalog {
public:
	explicit IRCatalog(AttachedDatabase &db_p, AccessMode access_mode, IRCCredentials credentials, string warehouse,
	                   string host, string secret_name, string version = "v1");
	~IRCatalog();

	string internal_name;
	AccessMode access_mode;
	IRCCredentials credentials;
	IRCEndpointBuilder endpoint_builder;

	//! warehouse
	string warehouse;
	//! host of the endpoint, like `glue` or `polaris`
	string host;
	//! secret name that Iceberg catalog should use for authentication
	string secret_name;
	//! version
	string version;
	//! optional prefix
	string prefix;

	ICEBERG_CATALOG_TYPE catalog_type = ICEBERG_CATALOG_TYPE::INVALID;

public:
	void Initialize(bool load_builtin) override;

	string GetCatalogType() override {
		return "iceberg";
	}

	static unique_ptr<SecretEntry> GetSecret(ClientContext &context, const string &secret_name);

	void GetConfig(ClientContext &context);

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> GetSchema(CatalogTransaction transaction, const string &schema_name,
	                                           OnEntryNotFound if_not_found,
	                                           QueryErrorContext error_context = QueryErrorContext()) override;

	unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
	                                               unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	IRCEndpointBuilder GetBaseUrl() const;

	//! Whether or not this is an in-memory PC database
	bool InMemory() override;
	string GetDBPath() override;

	void ClearCache();

	bool HasCachedValue(string url) const;
	string GetCachedValue(string url) const;
	bool SetCachedValue(string url, string value);

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	IRCSchemaSet schemas;
	string default_schema;

	// defaults and overrides provided by a catalog.
	unordered_map<string, string> defaults;
	unordered_map<string, string> overrides;

	unordered_map<string, unique_ptr<MetadataCacheValue>> metadata_cache;
};

} // namespace duckdb
