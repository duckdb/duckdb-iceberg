#include "maintenance/maintenance_table_loader.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/exception.hpp"

#include "catalog/rest/catalog_entry/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/iceberg_schema_set.hpp"
#include "catalog/rest/iceberg_table_set.hpp"

namespace duckdb {

shared_ptr<IcebergTableInformation> LoadIcebergTableShared(ClientContext &context, const MaintenanceTableKey &key,
                                                           const string &function_name) {
	auto &catalog = Catalog::GetCatalog(context, key.catalog);
	if (catalog.GetCatalogType() != "iceberg") {
		throw InvalidInputException(
		    "%s: catalog '%s' is not an Iceberg catalog (type='%s')",
		    function_name, key.catalog, catalog.GetCatalogType());
	}
	auto &iceberg_catalog = catalog.Cast<IcebergCatalog>();

	auto &schema_set = iceberg_catalog.GetSchemas();
	schema_set.LoadEntries(context);
	auto schema_entry_opt = schema_set.GetEntry(context, key.schema, OnEntryNotFound::RETURN_NULL);
	if (!schema_entry_opt) {
		throw InvalidInputException("%s: schema '%s' not found in catalog '%s'",
		                            function_name, key.schema, key.catalog);
	}
	auto &iceberg_schema = schema_entry_opt->Cast<IcebergSchemaEntry>();

	auto &tables = iceberg_schema.tables;
	tables.LoadEntries(context);
	auto &entries = tables.GetEntriesMutable();
	auto it = entries.find(key.table);
	if (it == entries.end()) {
		throw InvalidInputException("%s: table '%s' not found in schema '%s.%s'",
		                            function_name, key.table, key.catalog, key.schema);
	}

	auto table_info = it->second;
	if (!tables.FillEntry(context, *table_info)) {
		throw InvalidInputException("%s: failed to load table metadata for '%s.%s.%s'",
		                            function_name, key.catalog, key.schema, key.table);
	}
	return table_info;
}

IcebergTableInformation &LoadIcebergTable(ClientContext &context, const MaintenanceTableKey &key,
                                          const string &function_name) {
	return *LoadIcebergTableShared(context, key, function_name);
}

} // namespace duckdb
