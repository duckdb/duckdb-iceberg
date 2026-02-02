
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "iceberg_metadata.hpp"
#include "url_utils.hpp"
#include "storage/catalog/iceberg_table_entry.hpp"
#include "rest_catalog/objects/table_identifier.hpp"
#include "rest_catalog/objects/load_table_result.hpp"

namespace duckdb {

class IcebergCatalog;
class IcebergSchemaEntry;

struct IRCAPISchema {
	//! The (potentially multiple) levels that the namespace is made up of
	vector<string> items;
	string catalog_name;
};

enum class IRCEntryLookupStatus : uint8_t { EXISTS = 0, NOT_FOUND = 1, API_ERROR = 2 };

// Some API responses have error messages that need to be checked before being raised
// to the user, since sometimes is does not mean whole operation has failed.
// Ex: Glue will return an error when trying to get the metadata for a non-iceberg table during a list tables operation
//     The complete operation did not fail, just getting metadata for one table
template <typename T>
class APIResult {
public:
	APIResult() {};

	T result_;
	HTTPStatusCode status_;
	bool has_error;
	rest_api_objects::IcebergErrorResponse error_;
};

class IRCAPI {
public:
	static const string API_VERSION_1;
	static vector<rest_api_objects::TableIdentifier> GetTables(ClientContext &context, IcebergCatalog &catalog,
	                                                           const IcebergSchemaEntry &schema);
	static bool VerifyResponse(ClientContext &context, IcebergCatalog &catalog, IRCEndpointBuilder &url_builder,
	                           bool execute_head);
	static bool VerifySchemaExistence(ClientContext &context, IcebergCatalog &catalog, const string &schema);
	static bool VerifyTableExistence(ClientContext &context, IcebergCatalog &catalog, const IcebergSchemaEntry &schema,
	                                 const string &table);
	static vector<string> ParseSchemaName(const string &namespace_name);
	static string GetSchemaName(const vector<string> &items);
	static string GetEncodedSchemaName(const vector<string> &items);
	static APIResult<unique_ptr<const rest_api_objects::LoadTableResult>> GetTable(ClientContext &context,
	                                                                               IcebergCatalog &catalog,
	                                                                               const IcebergSchemaEntry &schema,
	                                                                               const string &table_name);
	static vector<IRCAPISchema> GetSchemas(ClientContext &context, IcebergCatalog &catalog,
	                                       const vector<string> &parent);
	static void CommitTableUpdate(ClientContext &context, IcebergCatalog &catalog, const vector<string> &schema,
	                              const string &table_name, const string &body);
	static void CommitTableDelete(ClientContext &context, IcebergCatalog &catalog, const vector<string> &schema,
	                              const string &table_name);
	static void CommitMultiTableUpdate(ClientContext &context, IcebergCatalog &catalog, const string &body);
	static void CommitNamespaceCreate(ClientContext &context, IcebergCatalog &catalog, string body);
	static void CommitNamespaceDrop(ClientContext &context, IcebergCatalog &catalog, vector<string> namespace_items);
	//! stage create = false, table is created immediately in the IRC
	//! stage create = true, table is not created, but metadata is initialized and returned
	static rest_api_objects::LoadTableResult CommitNewTable(ClientContext &context, IcebergCatalog &catalog,
	                                                        const IcebergTableEntry *table);
	static rest_api_objects::CatalogConfig GetCatalogConfig(ClientContext &context, IcebergCatalog &catalog);
};

} // namespace duckdb
