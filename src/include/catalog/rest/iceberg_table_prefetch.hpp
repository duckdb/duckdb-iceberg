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
class LoadTableCachePublication;

//! Binding resolves the tables of a query one at a time, and each lookup of an Iceberg table waits for its own
//! LoadTable request. Before binding, Catalog::PrefetchEntries passes the tables a statement references, and this
//! schedules their LoadTable requests so they are in flight together.
class IcebergTablePrefetch {
public:
	IcebergTablePrefetch(ClientContext &context, IcebergCatalog &catalog);
	~IcebergTablePrefetch();

	//! Schedule LoadTable requests for the `tables` of this catalog that the transaction has not resolved yet
	static void PrefetchTables(ClientContext &context, IcebergCatalog &catalog, const vector<QualifiedName> &tables);
	//! Take the response of a prefetched LoadTable request for `table_key`, waiting for it if needed.
	//! Returns false when no request was scheduled for the table, or when it failed to run
	bool TryTakeResult(const string &table_key, IcebergLoadTableResult &result,
	                   unique_ptr<LoadTableCachePublication> &publication);

private:
	struct PendingLoad {
		unique_ptr<LoadTableCachePublication> publication;
		shared_ptr<IcebergRequestResult<IcebergLoadTableResult>> result;
	};

	IcebergRequestExecutor executor;
	unordered_map<string, PendingLoad> pending;
};

} // namespace duckdb
