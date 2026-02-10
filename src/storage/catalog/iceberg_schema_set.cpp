#include "catalog_api.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/catalog/iceberg_catalog.hpp"
#include "storage/catalog/iceberg_schema_set.hpp"
#include "storage/iceberg_transaction.hpp"

namespace duckdb {

IcebergSchemaSet::IcebergSchemaSet(Catalog &catalog) : catalog(catalog) {
}

optional_ptr<CatalogEntry> IcebergSchemaSet::GetEntry(ClientContext &context, const string &name,
                                                      OnEntryNotFound if_not_found) {
	lock_guard<mutex> l(entry_lock);
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);

	// If the schema was deleted in this transaction, treat it as non-existent
	if (iceberg_transaction.deleted_schemas.count(name)) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException("Schema '%s' does not exist", name);
	}

	// If the schema was created in this transaction, return the local entry directly
	if (iceberg_transaction.created_schemas.count(name)) {
		auto entry = entries.find(name);
		if (entry != entries.end()) {
			return entry->second.get();
		}
		throw InternalException("Schema '%s' was created in this transaction, but was no found in the local cache",
		                        name);
	}

	auto verify_existence = iceberg_transaction.looked_up_entries.insert(name).second;
	auto entry = entries.find(name);
	if (entry != entries.end()) {
		return entry->second.get();
	}
	if (!verify_existence) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException("Iceberg namespace by the name of '%s' does not exist", name);
	}
	if (entry == entries.end()) {
		CreateSchemaInfo info;
		// Look up existence of default schema to avoid lookup of `duckdb_*` tables
		if (name == DEFAULT_SCHEMA) {
			if (!IRCAPI::VerifySchemaExistence(context, ic_catalog, name)) {
				if (if_not_found == OnEntryNotFound::RETURN_NULL) {
					return nullptr;
				}
				throw CatalogException("default schema '%s' does not exist", name);
			}
		}
		info.schema = name;
		info.internal = false;
		auto schema_entry = make_uniq<IcebergSchemaEntry>(catalog, info);
		// we will not create entries with empty names
		if (name.empty()) {
			return nullptr;
		}
		CreateEntryInternal(context, std::move(schema_entry));
		entry = entries.find(name);
		D_ASSERT(entry != entries.end());
	}
	return entry->second.get();
}

void IcebergSchemaSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> l(entry_lock);
	LoadEntries(context);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

void IcebergSchemaSet::AddEntry(const string &name, unique_ptr<IcebergSchemaEntry> entry) {
	entries.insert(make_pair(name, std::move(entry)));
}

void IcebergSchemaSet::RemoveEntry(const string &name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(name);
}

CatalogEntry &IcebergSchemaSet::GetEntry(const string &name) {
	auto entry_it = entries.find(name);
	if (entry_it == entries.end()) {
		throw CatalogException("Schema '%s' does not exist", name);
	}
	auto &entry = entry_it->second;
	return *entry;
}

const case_insensitive_map_t<unique_ptr<CatalogEntry>> &IcebergSchemaSet::GetEntries() {
	return entries;
}

static string GetSchemaName(const vector<string> &items) {
	return StringUtil::Join(items, ".");
}

void IcebergSchemaSet::LoadEntries(ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	bool schema_listed = iceberg_transaction.called_list_schemas;
	if (schema_listed) {
		return;
	}
	auto schemas = IRCAPI::GetSchemas(context, ic_catalog, {});
	for (const auto &schema : schemas) {
		CreateSchemaInfo info;
		info.schema = GetSchemaName(schema.items);
		info.internal = false;
		auto schema_entry = make_uniq<IcebergSchemaEntry>(catalog, info);
		schema_entry->namespace_items = std::move(schema.items);
		CreateEntryInternal(context, std::move(schema_entry));
	}
	iceberg_transaction.called_list_schemas = true;
}

optional_ptr<CatalogEntry> IcebergSchemaSet::CreateEntryInternal(ClientContext &context,
                                                                 unique_ptr<CatalogEntry> entry) {
	auto result = entry.get();
	if (result->name.empty()) {
		throw InternalException("IcebergSchemaSet::CreateEntry called with empty name");
	}
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}

} // namespace duckdb
