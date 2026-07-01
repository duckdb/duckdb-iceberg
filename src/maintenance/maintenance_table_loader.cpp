#include "maintenance/maintenance_table_loader.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/exception.hpp"

#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/iceberg_schema_set.hpp"
#include "catalog/rest/iceberg_table_set.hpp"

namespace duckdb {

namespace {

static IcebergSchemaEntry &LoadIcebergSchema(ClientContext &context, const QualifiedName &table_name,
                                             const string &function_name) {
	auto &catalog = Catalog::GetCatalog(context, table_name.catalog);
	if (catalog.GetCatalogType() != "iceberg") {
		throw InvalidInputException("%s: catalog '%s' is not an Iceberg catalog (type='%s')", function_name,
		                            table_name.catalog.GetIdentifierName(), catalog.GetCatalogType());
	}
	auto &iceberg_catalog = catalog.Cast<IcebergCatalog>();

	auto &schema_set = iceberg_catalog.GetSchemas();
	schema_set.LoadEntries(context);
	auto schema_entry_opt =
	    schema_set.GetEntry(context, table_name.schema.GetIdentifierName(), OnEntryNotFound::RETURN_NULL);
	if (!schema_entry_opt) {
		throw InvalidInputException("%s: schema '%s' not found in catalog '%s'", function_name,
		                            table_name.schema.GetIdentifierName(), table_name.catalog.GetIdentifierName());
	}
	return schema_entry_opt->Cast<IcebergSchemaEntry>();
}

} // namespace

shared_ptr<IcebergTableInformation> ReloadIcebergTableShared(ClientContext &context, const QualifiedName &table_name,
                                                             const string &function_name) {
	auto &iceberg_schema = LoadIcebergSchema(context, table_name, function_name);
	auto &tables = iceberg_schema.tables;
	auto table_name_string = table_name.name.GetIdentifierName();
	auto table_info = make_shared_ptr<IcebergTableInformation>(iceberg_schema.ParentCatalog().Cast<IcebergCatalog>(),
	                                                           iceberg_schema, table_name_string);
	if (!tables.FillEntry(context, *table_info)) {
		throw InvalidInputException("%s: table '%s' not found in schema '%s.%s'", function_name, table_name_string,
		                            table_name.catalog.GetIdentifierName(), table_name.schema.GetIdentifierName());
	}
	return table_info;
}

} // namespace duckdb
