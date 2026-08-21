#include "catalog/rest/iceberg_schema_set.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"

namespace duckdb {

static string GetSchemaName(const vector<string> &items) {
	return StringUtil::Join(items, ".");
}

IcebergSchemaSet::IcebergSchemaSet(Catalog &catalog, CaseSensitivityMode mode)
    : catalog(catalog), mode(mode), entries(mode) {
	fprintf(stderr, "DEBUGDEBUG IcebergSchemaSet ctor catalog='%s' mode=%d\n",
	        catalog.GetName().GetIdentifierName().c_str(), static_cast<int>(mode));
	fflush(stderr);
}

string IcebergSchemaSet::ResolveCanonicalNameViaList(ClientContext &context, const string &name) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto schemas = IRCAPI::GetSchemas(context, ic_catalog, {});
	string canonical_match;
	for (const auto &schema : schemas) {
		auto candidate = GetSchemaName(schema.items);
		if (!StringUtil::CIEquals(candidate, name)) {
			continue;
		}
		if (!canonical_match.empty() && canonical_match != candidate) {
			throw CatalogException(
			    "Ambiguous case-insensitive namespace reference '%s': matches both '%s' and '%s'. Use "
			    "case_sensitive=true or an exact-case reference to disambiguate.",
			    name, canonical_match, candidate);
		}
		canonical_match = candidate;
	}
	return canonical_match;
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

	auto verify_existence = iceberg_transaction.looked_up_entries.insert_permissive(name);
	auto entry = entries.find(name);
	if (entry != entries.end()) {
		iceberg_transaction.schemas.emplace(entry->first, entry->second);
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
		string resolved_name = name;
		if (mode == CaseSensitivityMode::INSENSITIVE) {
			resolved_name = ResolveCanonicalNameViaList(context, name);
			if (resolved_name.empty()) {
				if (if_not_found == OnEntryNotFound::RETURN_NULL) {
					return nullptr;
				}
				throw CatalogException("Iceberg namespace by the name of '%s' does not exist", name);
			}
			entry = entries.find(resolved_name);
			if (entry != entries.end()) {
				iceberg_transaction.schemas.emplace(entry->first, entry->second);
				return entry->second->DoesExist() ? entry->second.get() : nullptr;
			}
		}
		// Look up existence of default schema to avoid lookup of `duckdb_*` tables
		if (resolved_name == DEFAULT_SCHEMA) {
			if (!IRCAPI::VerifySchemaExistence(context, ic_catalog, resolved_name)) {
				if (if_not_found == OnEntryNotFound::RETURN_NULL) {
					return nullptr;
				}
				throw CatalogException("default schema '%s' does not exist", resolved_name);
			}
		}
		info.SetQualifiedName(QualifiedName(info.GetQualifiedName().Catalog(), Identifier(resolved_name),
		                                    info.GetQualifiedName().Name()));
		info.internal = false;
		auto schema_entry = make_shared_ptr<IcebergSchemaEntry>(catalog, info);
		// we will not create entries with empty names
		if (resolved_name.empty()) {
			return nullptr;
		}
		auto inserted_entry = CreateEntryInternal(std::move(schema_entry));
		iceberg_transaction.schemas.emplace(resolved_name, inserted_entry);
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
	auto insert_result = entries.insert(name, entry);
	if (!insert_result.second) {
		insert_result.first->second = std::move(entry);
	}
}

void IcebergSchemaSet::RemoveEntry(const string &name) {
	annotated_lock_guard<annotated_mutex> l(entry_lock);
	entries.erase(name);
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
		fprintf(stderr, "DEBUGDEBUG SchemaSet::LoadEntriesInternal catalog='%s' mode=%d namespace='%s'\n",
		        catalog.GetName().GetIdentifierName().c_str(), static_cast<int>(mode),
		        GetSchemaName(schema.items).c_str());
		fflush(stderr);
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
	auto insert_result = entries.insert(name, entry);
	if (insert_result.second) {
		return insert_result.first->second;
	}
	auto existing_entry = insert_result.first;
	if (!existing_entry->second->DoesExist()) {
		existing_entry->second = std::move(entry);
	}
	return existing_entry->second;
}

} // namespace duckdb
