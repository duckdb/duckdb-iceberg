//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/iceberg_table_prefetch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/iceberg_request_executor.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

class IcebergCatalog;
class IcebergTransaction;
class SQLStatement;
class LoadTableCachePublication;

//! Binding resolves the tables of a query one at a time, and each lookup of an Iceberg table waits for its own
//! LoadTable request. Before binding, the planner extension's pre-bind function passes the statement, and this
//! runs the LoadTable requests of the Iceberg tables it references together, so binding finds their responses ready.
class IcebergTablePrefetch {
public:
	IcebergTablePrefetch(ClientContext &context, IcebergCatalog &catalog);
	~IcebergTablePrefetch();

	//! Schedule LoadTable requests for the Iceberg tables `statement` references, before it is bound. Errors other than
	//! interrupts are ignored: binding still looks up every table on its own
	static void PrefetchStatementTables(ClientContext &context, SQLStatement &statement);
	//! Run LoadTable requests for the `tables` of this catalog that the transaction has not resolved yet, and wait
	//! for all of them
	static void PrefetchTables(ClientContext &context, IcebergCatalog &catalog, const vector<QualifiedName> &tables);
	//! Take the response of a prefetched LoadTable request for `table_key`, waiting for it if needed.
	//! Returns false when no request was scheduled for the table, or when it failed to run
	bool TryTakeResult(const string &table_key, IcebergLoadTableResult &result,
	                   unique_ptr<LoadTableCachePublication> &publication);

private:
	static void ScheduleTables(ClientContext &context, IcebergCatalog &catalog, IcebergTransaction &transaction,
	                           const vector<QualifiedName> &tables);

private:
	struct PendingLoad {
		unique_ptr<LoadTableCachePublication> publication;
		shared_ptr<IcebergRequestResult<IcebergLoadTableResult>> result;
	};

	IcebergRequestExecutor executor;
	unordered_map<string, PendingLoad> pending;
};

} // namespace duckdb
