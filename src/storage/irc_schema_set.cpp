#include "catalog_api.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_schema_set.hpp"
#include "storage/irc_transaction.hpp"

namespace duckdb {

IRCSchemaSet::IRCSchemaSet(Catalog &catalog) : IRCCatalogSet(catalog) {
}

void IRCSchemaSet::LoadEntries(ClientContext &context) {
	if (!entries.empty()) {
		return;
	}

	auto &ic_catalog = catalog.Cast<IRCatalog>();
	auto schemas = IRCAPI::GetSchemas(context, ic_catalog);
	for (const auto &schema : schemas) {
		CreateSchemaInfo info;
		info.schema = schema.schema_name;
		info.internal = false;
		auto schema_entry = make_uniq<IRCSchemaEntry>(catalog, info);
		schema_entry->schema_data = make_uniq<IRCAPISchema>(schema);
		CreateEntry(std::move(schema_entry));
	}
}

void IRCSchemaSet::FillEntry(ClientContext &context, unique_ptr<CatalogEntry> &entry) {
	// Nothing to do
}

optional_ptr<CatalogEntry> IRCSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	auto schema = IRCAPI::CreateSchema(context, ic_catalog, ic_catalog.internal_name, info.schema);
	auto schema_entry = make_uniq<IRCSchemaEntry>(catalog, info);
	schema_entry->schema_data = make_uniq<IRCAPISchema>(schema);
	return CreateEntry(std::move(schema_entry));
}

void IRCSchemaSet::DropSchema(ClientContext &context, DropInfo &info) {
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	IRCAPI::DropSchema(context, ic_catalog.internal_name, info.name);
	DropEntry(context, info);
}

} // namespace duckdb
