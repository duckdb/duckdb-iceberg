
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {
struct ICCredentials;

struct ICAPIColumnDefinition {
	string name;
	string type_text;
	idx_t precision;
	idx_t scale;
	idx_t position;
};

struct ICAPITable {
	string table_id;

	string name;
	string catalog_name;
	string schema_name;
	string table_type;
	string data_source_format;
	string storage_location;

	vector<ICAPIColumnDefinition> columns;
};

struct ICAPISchema {
	string schema_name;
	string catalog_name;
};

struct ICAPITableCredentials {
	string key_id;
	string secret;
	string session_token;
};

class ICAPI {
public:
  	//! WARNING: not thread-safe. To be called once on extension initialization
  	static void InitializeCurl();

	static ICAPITableCredentials GetTableCredentials(const string &internal, const string &schema, const string &table, ICCredentials credentials);
	static vector<string> GetCatalogs(const string &catalog, ICCredentials credentials);
	static vector<ICAPITable> GetTables(const string &catalog, const string &internal, const string &schema, ICCredentials credentials);
	static ICAPITable GetTable(const string &catalog, const string &internal, const string &schema, const string &table_name, std::optional<ICCredentials> credentials);
	static vector<ICAPISchema> GetSchemas(const string &catalog, const string &internal, ICCredentials credentials);
	static vector<ICAPITable> GetTablesInSchema(const string &catalog, const string &schema, ICCredentials credentials);
	static string GetToken(string id, string secret, string endpoint);
	static ICAPISchema CreateSchema(const string &catalog, const string &internal, const string &schema, ICCredentials credentials);
	static void DropSchema(const string &internal, const string &schema, ICCredentials credentials);
	static ICAPITable CreateTable(const string &catalog, const string &internal, const string &schema, ICCredentials &credentials, CreateTableInfo *table_info);
	static void DropTable(const string &catalog, const string &internal, const string &schema, string &table_name, ICCredentials credentials);
};

} // namespace duckdb
