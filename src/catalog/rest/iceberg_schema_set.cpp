#include "catalog/rest/iceberg_schema_set.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/iceberg_request_executor.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"

namespace duckdb {

IcebergSchemaSet::IcebergSchemaSet(Catalog &catalog) : catalog(catalog) {
}

optional_ptr<CatalogEntry> IcebergSchemaSet::GetEntry(ClientContext &context, const string &name,
                                                      OnEntryNotFound if_not_found) {
	annotated_lock_guard<annotated_mutex> l(entry_lock);
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);

	if (name.empty()) {
		return nullptr;
	}

	// If the schema was deleted in this transaction, treat it as non-existent
	if (iceberg_transaction.deleted_schemas.count(name)) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException("Schema '%s' does not exist", name);
	}

	// Transaction-local creations take precedence over catalog entries with the same name.
	auto created_schema = iceberg_transaction.created_schemas.GetEntry(name);
	if (created_schema) {
		return created_schema.get();
	}

	// Track explicit lookups even when a discovery scan already pinned the schema.
	auto verify_existence = iceberg_transaction.looked_up_entries.insert(name).second;
	// Return an entry already referenced by this transaction directly.
	auto transaction_entry = iceberg_transaction.schemas.find(name);
	if (transaction_entry != iceberg_transaction.schemas.end()) {
		if (transaction_entry->second->DoesExist()) {
			return transaction_entry->second.get();
		}
		return nullptr;
	}

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
		auto default_schema = ic_catalog.GetDefaultSchema();
		auto lookup_name = Identifier(name);
		if (lookup_name == default_schema) {
			// Verify the default schema does exist
			if (!IRCAPI::VerifySchemaExistence(context, ic_catalog, name)) {
				if (if_not_found == OnEntryNotFound::RETURN_NULL) {
					return nullptr;
				}
				if (lookup_name == default_schema) {
					throw CatalogException(
					    "default namespace '%s' does not exist in this Iceberg catalog - create it, or attach "
					    "with DEFAULT_SCHEMA '<namespace>'",
					    name);
				}
				throw CatalogException("Iceberg namespace by the name of '%s' does not exist", name);
			}
		}
		// assume schema exists to avoid extra roundtrip
		auto schema_entry = MakeEntryInternal(iceberg_transaction, IRCAPI::ParseSchemaName(name));
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
	result.reserve(entries.size() + iceberg_transaction.created_schemas.Size());
	for (auto &entry : entries) {
		if (iceberg_transaction.deleted_schemas.count(entry.first) ||
		    iceberg_transaction.created_schemas.Contains(entry.first)) {
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
		result.push_back(created_schema.second.entry);
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

shared_ptr<IcebergSchemaEntry> IcebergSchemaSet::MakeEntry(ClientContext &context, const string &name) {
	annotated_lock_guard<annotated_mutex> l(entry_lock);
	return MakeEntryInternal(IcebergTransaction::Get(context, catalog), IRCAPI::ParseSchemaName(name));
}

shared_ptr<IcebergSchemaEntry> IcebergSchemaSet::MakeEntryInternal(IcebergTransaction &transaction,
                                                                   vector<string> namespace_items) {
	CreateSchemaInfo info;
	info.internal = false;
	shared_ptr<IcebergSchemaEntry> parent;
	// a namespace is named by its last level and reaches the other levels through its parent
	Identifier name(namespace_items.back());
	if (namespace_items.size() > 1) {
		vector<string> parent_items(namespace_items.begin(), namespace_items.end() - 1);
		auto parent_key = StringUtil::Join(parent_items, ".");
		parent = transaction.created_schemas.GetSharedEntry(parent_key);
		if (!parent) {
			auto transaction_parent = transaction.schemas.find(parent_key);
			auto catalog_parent = entries.find(parent_key);
			if (transaction_parent != transaction.schemas.end()) {
				parent = transaction_parent->second;
			} else if (catalog_parent != entries.end()) {
				parent = catalog_parent->second;
			} else {
				parent = MakeEntryInternal(transaction, std::move(parent_items));
			}
		}
	}
	info.SetQualifiedName(QualifiedName(info.GetQualifiedName().Catalog(), name, info.GetQualifiedName().Name()));
	return make_shared_ptr<IcebergSchemaEntry>(catalog, info, std::move(namespace_items), std::move(parent));
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
	// The local executor drains on scope exit; the task retains its result storage until it finishes.
	IcebergRequestExecutor executor(context, ic_catalog);
	auto result = executor.Schedule(IcebergListSchemasRequest({}));
	auto schemas = executor.WaitAndTakeResult(*result);
	if (context.IsInterrupted()) {
		throw InterruptException();
	}
	ApplyListResult(iceberg_transaction, std::move(schemas));
	iceberg_transaction.called_list_schemas = true;
}

void IcebergSchemaSet::ApplyListResult(IcebergTransaction &transaction, IcebergListSchemasResult schemas) {
	// parents first, so that a nested namespace links to the entry of its parent
	std::stable_sort(schemas.begin(), schemas.end(), [](const IRCAPISchema &lhs, const IRCAPISchema &rhs) {
		return lhs.items.size() < rhs.items.size();
	});
	for (auto &schema : schemas) {
		if (schema.items.empty() ||
		    std::any_of(schema.items.begin(), schema.items.end(), [](const string &item) { return item.empty(); })) {
			throw InvalidConfigurationException("Iceberg catalog returned an empty namespace name");
		}
		CreateEntryInternal(MakeEntryInternal(transaction, std::move(schema.items)));
	}
}

shared_ptr<IcebergSchemaEntry> IcebergSchemaSet::CreateEntryInternal(shared_ptr<IcebergSchemaEntry> entry) {
	auto name = entry->GetNamespaceKey();
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
