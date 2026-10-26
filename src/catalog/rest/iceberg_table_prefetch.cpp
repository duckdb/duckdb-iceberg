#include "catalog/rest/iceberg_table_prefetch.hpp"

#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"

namespace duckdb {

IcebergTablePrefetch::IcebergTablePrefetch(ClientContext &context, IcebergCatalog &catalog)
    : executor(context, catalog) {
}

IcebergTablePrefetch::~IcebergTablePrefetch() {
}

void IcebergTablePrefetch::PrefetchTables(ClientContext &context, IcebergCatalog &catalog,
                                          const vector<QualifiedName> &tables) {
	auto &transaction = IcebergTransaction::Get(context, catalog);
	for (auto &name : tables) {
		auto &table_name = name.Name().GetIdentifierName();
		if (table_name.find_first_of("/\\") != string::npos) {
			//! A file path of a replacement scan, e.g. FROM 's3://bucket/file.parquet'. IcebergTableSet::GetEntry
			//! treats these names the same way
			continue;
		}
		auto schema_entry =
		    catalog.GetSchemas().GetEntry(context, name.Schema().GetIdentifierName(), OnEntryNotFound::RETURN_NULL);
		if (!schema_entry) {
			continue;
		}
		auto &schema = schema_entry->Cast<IcebergSchemaEntry>();
		auto table_key = IcebergTable::GetTableKey(catalog, schema.namespace_items, table_name);
		if (transaction.GetLatestTableState(table_key)) {
			continue;
		}
		auto existing = transaction.TryGetTablePrefetch();
		if (existing && existing->pending.count(table_key)) {
			continue;
		}
		if (catalog.attach_options.max_table_staleness_micros.IsValid() &&
		    catalog.table_request_cache.Get(context, table_key, [](const rest_api_objects::LoadTableResult &) {})) {
			continue;
		}
		auto &prefetch = transaction.GetTablePrefetch(context);
		PendingLoad load;
		load.publication = catalog.table_request_cache.BeginLoad(table_key);
		load.result = prefetch.executor.Schedule(IcebergLoadTableRequest(schema.namespace_items, table_name));
		prefetch.pending.emplace(std::move(table_key), std::move(load));
	}
}

bool IcebergTablePrefetch::TryTakeResult(const string &table_key, IcebergLoadTableResult &result,
                                         unique_ptr<LoadTableCachePublication> &publication) {
	auto entry = pending.find(table_key);
	if (entry == pending.end()) {
		return false;
	}
	auto load = std::move(entry->second);
	pending.erase(entry);
	try {
		result = executor.WaitAndTakeResult(*load.result);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERRUPT) {
			throw;
		}
		//! The caller requests the table itself
		return false;
	}
	publication = std::move(load.publication);
	return true;
}

} // namespace duckdb
