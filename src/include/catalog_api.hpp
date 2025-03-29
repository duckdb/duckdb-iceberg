
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
//#include "storage/irc_catalog.hpp"

namespace duckdb {

class IRCatalog;
struct IRCCredentials;

struct IRCAPIColumnDefinition {
	string name;
	string type_text;
	idx_t precision;
	idx_t scale;
	idx_t position;
};

struct IRCAPITable {
	string table_id;

	string name;
	string catalog_name;
	string schema_name;
	string table_type;
	string data_source_format;
	string storage_location;

	vector<IRCAPIColumnDefinition> columns;
};

struct IRCAPISchema {
	string schema_name;
	string catalog_name;
};

struct IRCAPITableCredentials {
	unique_ptr<CreateSecretInfo> config;
	vector<CreateSecretInfo> storage_credentials;
};

class IRCAPI {
public:
	static const string API_VERSION_1;

	//! WARNING: not thread-safe. To be called once on extension initialization
	static void InitializeCurl();

	static IRCAPITableCredentials GetTableCredentials(ClientContext &context, IRCatalog &catalog, const string &schema,
	                                                  const string &table, const string &secret_base_name);
	static vector<string> GetCatalogs(ClientContext &context, IRCatalog &catalog, IRCCredentials credentials);
	static vector<IRCAPITable> GetTables(ClientContext &context, IRCatalog &catalog, const string &schema);
	static IRCAPITable GetTable(ClientContext &context, IRCatalog &catalog, const string &schema,
	                            const string &table_name, optional_ptr<IRCCredentials> credentials = nullptr);
	static vector<IRCAPISchema> GetSchemas(ClientContext &context, IRCatalog &catalog, IRCCredentials credentials);
	static string GetToken(ClientContext &context, const string &uri, const string &id, const string &secret,
	                       const string &endpoint, const string &scope);
	static IRCAPISchema CreateSchema(ClientContext &context, IRCatalog &catalog, const string &internal,
	                                 const string &schema, IRCCredentials credentials);
	static void DropSchema(ClientContext &context, const string &internal, const string &schema,
	                       IRCCredentials credentials);
	static IRCAPITable CreateTable(ClientContext &context, IRCatalog &catalog, const string &internal,
	                               const string &schema, IRCCredentials credentials, CreateTableInfo *table_info);
	static void DropTable(ClientContext &context, IRCatalog &catalog, const string &internal, const string &schema,
	                      string &table_name, IRCCredentials credentials);
};

} // namespace duckdb
