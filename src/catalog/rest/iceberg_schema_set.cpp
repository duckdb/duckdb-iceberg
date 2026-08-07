#include "catalog/rest/iceberg_schema_set.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"

namespace duckdb {

IcebergSchemaSet::IcebergSchemaSet(Catalog &catalog) : catalog(catalog) {
}

optional_ptr<CatalogEntry> IcebergSchemaSet::GetEntry(ClientContext &context, const string &name,
                                                      OnEntryNotFound if_not_found) {
	annotated_lock_guard<annotated_mutex> l(entry_lock);
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);

	// If the schema was deleted in this transaction, treat it as non-existent
	if (iceberg_transaction.deleted_schemas.count(name)) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException("Schema '%s' does not exist", name);
	}

	// Transaction-local creations take precedence over catalog entries with the same name.
	auto created_schema = iceberg_transaction.created_schemas.find(name);
	if (created_schema != iceberg_transaction.created_schemas.end()) {
		return created_schema->second.get();
	}

	// Return an entry already referenced by this transaction directly.
	auto transaction_entry = iceberg_transaction.schemas.find(name);
	if (transaction_entry != iceberg_transaction.schemas.end()) {
		if (transaction_entry->second->DoesExist()) {
			return transaction_entry->second.get();
		}
		return nullptr;
	}

	auto verify_existence = iceberg_transaction.looked_up_entries.insert(name).second;
	auto entry = entries.find(name);
	if (entry != entries.end()) {
		iceberg_transaction.schemas.emplace(name, entry->second);
		if (entry->second->DoesExist()) {
			return entry->second.get();
		}
		return nullptr;
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
		info.SetQualifiedName(
		    QualifiedName(info.GetQualifiedName().Catalog(), Identifier(name), info.GetQualifiedName().Name()));
		info.internal = false;
		auto schema_entry = make_shared_ptr<IcebergSchemaEntry>(catalog, info);
		// we will not create entries with empty names
		if (name.empty()) {
			return nullptr;
		}
		auto inserted_entry = CreateEntryInternal(std::move(schema_entry));
		iceberg_transaction.schemas.emplace(name, inserted_entry);
		return inserted_entry.get();
	}
	iceberg_transaction.schemas.emplace(name, entry->second);
	return entry->second.get();
}

void IcebergSchemaSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	auto schema_entries = GetEntries(context);
	for (auto &entry : schema_entries) {
		callback(*entry);
	}
}

vector<shared_ptr<IcebergSchemaEntry>> IcebergSchemaSet::GetEntries(ClientContext &context) {
	annotated_lock_guard<annotated_mutex> l(entry_lock);
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	LoadEntriesInternal(context);
	vector<shared_ptr<IcebergSchemaEntry>> result;
	result.reserve(entries.size() + iceberg_transaction.created_schemas.size());
	for (auto &entry : entries) {
		if (iceberg_transaction.deleted_schemas.count(entry.first) ||
		    iceberg_transaction.created_schemas.count(entry.first)) {
			continue;
		}
		auto transaction_entry = iceberg_transaction.schemas.find(entry.first);
		if (transaction_entry == iceberg_transaction.schemas.end()) {
			transaction_entry = iceberg_transaction.schemas.emplace(entry.first, entry.second).first;
		}
		if (transaction_entry->second->DoesExist()) {
			result.push_back(transaction_entry->second);
		}
	}
	for (auto &created_schema : iceberg_transaction.created_schemas) {
		result.push_back(created_schema.second);
	}
	return result;
}

void IcebergSchemaSet::AddEntry(const string &name, shared_ptr<IcebergSchemaEntry> entry) {
	D_ASSERT(entry);
	annotated_lock_guard<annotated_mutex> l(entry_lock);
	entries[name] = std::move(entry);
}

void IcebergSchemaSet::RemoveEntry(const string &name) {
	annotated_lock_guard<annotated_mutex> l(entry_lock);
	entries.erase(name);
}

static string GetSchemaName(const vector<string> &items) {
	return StringUtil::Join(items, ".");
}

void IcebergSchemaSet::LoadEntries(ClientContext &context) {
	annotated_lock_guard<annotated_mutex> l(entry_lock);
	LoadEntriesInternal(context);
}

void IcebergSchemaSet::LoadEntriesInternal(ClientContext &context) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	bool schema_listed = iceberg_transaction.called_list_schemas;
	if (schema_listed) {
		return;
	}
	auto schemas = IRCAPI::GetSchemas(context, ic_catalog, {});
	for (const auto &schema : schemas) {
		CreateSchemaInfo info;
		info.SetQualifiedName(QualifiedName(info.GetQualifiedName().Catalog(), Identifier(GetSchemaName(schema.items)),
		                                    info.GetQualifiedName().Name()));
		info.internal = false;
		auto schema_entry = make_shared_ptr<IcebergSchemaEntry>(catalog, info);
		schema_entry->namespace_items = std::move(schema.items);
		CreateEntryInternal(std::move(schema_entry));
	}
	iceberg_transaction.called_list_schemas = true;
}

shared_ptr<IcebergSchemaEntry> IcebergSchemaSet::CreateEntryInternal(shared_ptr<IcebergSchemaEntry> entry) {
	auto &name = entry->name.GetIdentifierName();
	if (name.empty()) {
		throw InternalException("IcebergSchemaSet::CreateEntry called with empty name");
	}
	auto existing_entry = entries.find(name);
	if (existing_entry == entries.end()) {
		return entries.emplace(name, std::move(entry)).first->second;
	}
	if (!existing_entry->second->DoesExist()) {
		existing_entry->second = std::move(entry);
	}
	return existing_entry->second;
}

} // namespace duckdb
