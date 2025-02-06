#include "storage/ic_catalog_set.hpp"
#include "storage/ic_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/ic_schema_entry.hpp"

namespace duckdb {

ICCatalogSet::ICCatalogSet(Catalog &catalog) : catalog(catalog) {
}

optional_ptr<CatalogEntry> ICCatalogSet::GetEntry(ClientContext &context, const string &name) {
	LoadEntries(context);
	lock_guard<mutex> l(entry_lock);
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	FillEntry(context, entry->second);
	return entry->second.get();
}

void ICCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
	EraseEntryInternal(info.name);
}

void ICCatalogSet::EraseEntryInternal(const string &name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(name);
}

void ICCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	LoadEntries(context);

	lock_guard<mutex> l(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> ICCatalogSet::AddEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(entry_lock);
	auto result = entry.get();
	if (result->name.empty()) {
		throw InternalException("ICCatalogSet::CreateEntry called with empty name");
	}
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}

void ICCatalogSet::ClearEntries() {
	entries.clear();
}

} // namespace duckdb
