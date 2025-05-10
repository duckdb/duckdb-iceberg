
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "iceberg_metadata.hpp"
//#include "storage/irc_catalog.hpp"

namespace duckdb {

class IRCatalog;

struct IRCAPITable {
public:
	IRCAPITable(const string &table_id) : table_id(table_id) {
	}
	IRCAPITable() = default;

	// Enable move constructor and move assignment
	IRCAPITable(IRCAPITable &&) noexcept = default;
	IRCAPITable &operator=(IRCAPITable &&) noexcept = default;

	// Disable copy constructor and copy assignment
	IRCAPITable(const IRCAPITable &) = delete;
	IRCAPITable &operator=(const IRCAPITable &) = delete;

public:
	string table_id;

	string name;
	string catalog_name;
	string schema_name;
	string table_type;
	string data_source_format;
	string storage_location;

	vector<unique_ptr<IcebergColumnDefinition>> columns;
};

struct IRCAPISchema {
	string schema_name;
	string catalog_name;
};

struct IRCAPITableCredentials {
	unique_ptr<CreateSecretInput> config;
	vector<CreateSecretInput> storage_credentials;
};

class IRCAPI {
public:
	static const string API_VERSION_1;

	static IRCAPITableCredentials GetTableCredentials(ClientContext &context, IRCatalog &catalog, const string &schema,
	                                                  const string &table, const string &secret_base_name);
	static vector<string> GetCatalogs(ClientContext &context, IRCatalog &catalog);
	static vector<IRCAPITable> GetTables(ClientContext &context, IRCatalog &catalog, const string &schema);
	static IRCAPITable GetTable(ClientContext &context, IRCatalog &catalog, const string &schema,
	                            const string &table_name, bool perform_request = false);
	static vector<IRCAPISchema> GetSchemas(ClientContext &context, IRCatalog &catalog);

	static IRCAPISchema CreateSchema(ClientContext &context, IRCatalog &catalog, const string &internal,
	                                 const string &schema);
	static void DropSchema(ClientContext &context, const string &internal, const string &schema);
	static IRCAPITable CreateTable(ClientContext &context, IRCatalog &catalog, const string &internal,
	                               const string &schema, CreateTableInfo *table_info);
	static void DropTable(ClientContext &context, IRCatalog &catalog, const string &internal, const string &schema,
	                      string &table_name);
};

} // namespace duckdb
