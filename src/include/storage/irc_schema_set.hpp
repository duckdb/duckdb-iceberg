
#pragma once

#include "storage/irc_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class IRCSchemaSet {
public:
	explicit IRCSchemaSet(Catalog &catalog);

public:
	void LoadEntries(ClientContext &context);
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);

protected:
	optional_ptr<CatalogEntry> CreateEntryInternal(ClientContext &context, unique_ptr<CatalogEntry> entry);

public:
	Catalog &catalog;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;

private:
	mutex entry_lock;
};

} // namespace duckdb
