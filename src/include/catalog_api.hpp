
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "iceberg_metadata.hpp"
#include "storage/irc_table_entry.hpp"
#include "rest_catalog/objects/table_identifier.hpp"
#include "rest_catalog/objects/load_table_result.hpp"

namespace duckdb {

class IRCatalog;
class IRCSchemaEntry;

struct IRCAPISchema {
	//! The (potentially multiple) levels that the namespace is made up of
	vector<string> items;
	string catalog_name;
};

template <typename T>
class APIResult {
public:
	APIResult() {};

	T result_;
	bool has_error;
	rest_api_objects::IcebergErrorResponse error_;
};

class IRCAPI {
public:
	static const string API_VERSION_1;
	static vector<string> ParseSchemaName(string &namespace_name);
	static string GetSchemaName(const vector<string> &items);
	static string GetEncodedSchemaName(const vector<string> &items);
	static vector<rest_api_objects::TableIdentifier> GetTables(ClientContext &context, IRCatalog &catalog,
	                                                           const IRCSchemaEntry &schema);
	static APIResult<rest_api_objects::LoadTableResult>
	GetTable(ClientContext &context, IRCatalog &catalog, const IRCSchemaEntry &schema, const string &table_name);
	static vector<IRCAPISchema> GetSchemas(ClientContext &context, IRCatalog &catalog, const vector<string> &parent);
	static void CommitTableUpdate(ClientContext &context, IRCatalog &catalog, const vector<string> &schema,
	                              const string &table_name, const string &body);
	static void CommitTableDelete(ClientContext &context, IRCatalog &catalog, const vector<string> &schema,
	                              const string &table_name);
	static void CommitMultiTableUpdate(ClientContext &context, IRCatalog &catalog, const string &body);
	static void CommitNamespaceCreate(ClientContext &context, IRCatalog &catalog, string body);
	static void CommitNamespaceDrop(ClientContext &context, IRCatalog &catalog, vector<string> namespace_items);
	//! stage create = false, table is created immediately in the IRC
	//! stage create = true, table is not created, but metadata is initialized and returned
	static rest_api_objects::LoadTableResult CommitNewTable(ClientContext &context, IRCatalog &catalog,
	                                                        const ICTableEntry *table);
	static rest_api_objects::CatalogConfig GetCatalogConfig(ClientContext &context, IRCatalog &catalog);
};

} // namespace duckdb
