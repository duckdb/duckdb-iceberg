#include "catalog_api.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/ic_catalog.hpp"
#include "storage/ic_schema_set.hpp"
#include "storage/ic_transaction.hpp"

namespace duckdb {

ICSchemaSet::ICSchemaSet(Catalog &catalog) : ICCatalogSet(catalog) {
}

static bool IsInternalTable(const string &catalog, const string &schema) {
	if (schema == "information_schema") {
		return true;
	}
	return false;
}

void ICSchemaSet::LoadEntries(ClientContext &context) {
	if (!entries.empty()) {
		return;
	}

	auto &ic_catalog = catalog.Cast<ICCatalog>();
	auto schemas = ICAPI::GetSchemas(catalog.GetName(), ic_catalog.internal_name, ic_catalog.credentials);
	for (const auto &schema : schemas) {
		CreateSchemaInfo info;
		info.schema = schema.schema_name;
		info.internal = IsInternalTable(schema.catalog_name, schema.schema_name);
		auto schema_entry = make_uniq<ICSchemaEntry>(catalog, info);
		schema_entry->schema_data = make_uniq<ICAPISchema>(schema);
		AddEntry(std::move(schema_entry));
	}
}

void ICSchemaSet::FillEntry(ClientContext &context, unique_ptr<CatalogEntry> &entry) {
	// Nothing to do
}

optional_ptr<CatalogEntry> ICSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	auto &ic_catalog = catalog.Cast<ICCatalog>();
	auto schema = ICAPI::CreateSchema(catalog.GetName(), ic_catalog.internal_name, info.schema, ic_catalog.credentials);
	auto schema_entry = make_uniq<ICSchemaEntry>(catalog, info);
	schema_entry->schema_data = make_uniq<ICAPISchema>(schema);
	return AddEntry(std::move(schema_entry));
}

void ICSchemaSet::DropSchema(ClientContext &context, DropInfo &info) {
	auto &ic_catalog = catalog.Cast<ICCatalog>();
	ICAPI::DropSchema(ic_catalog.internal_name, info.name, ic_catalog.credentials);
	DropEntry(context, info);
}

} // namespace duckdb
