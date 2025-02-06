
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
struct DropInfo;
class ICSchemaEntry;
class ICTransaction;

class ICCatalogSet {
public:
	ICCatalogSet(Catalog &catalog);

	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);
	virtual void DropEntry(ClientContext &context, DropInfo &info);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	virtual optional_ptr<CatalogEntry> AddEntry(unique_ptr<CatalogEntry> entry);
	void ClearEntries();

protected:
	virtual void LoadEntries(ClientContext &context) = 0;
	virtual void FillEntry(ClientContext &context, unique_ptr<CatalogEntry> &entry) = 0;

	void EraseEntryInternal(const string &name);

protected:
	Catalog &catalog;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;

private:
	mutex entry_lock;
};

} // namespace duckdb
