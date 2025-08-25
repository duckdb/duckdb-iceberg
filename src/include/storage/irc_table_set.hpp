
#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "storage/irc_table_entry.hpp"
#include "storage/iceberg_table_information.hpp"
#include "storage/iceberg_transaction_data.hpp"

namespace duckdb {
struct CreateTableInfo;
class ICResult;
class IRCSchemaEntry;
class IRCTransaction;

class ICTableSet {
public:
	explicit ICTableSet(IRCSchemaEntry &schema);

public:
	static unique_ptr<ICTableInfo> GetTableInfo(ClientContext &context, IRCSchemaEntry &schema,
	                                            const string &table_name);
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	void CreateNewEntry(ClientContext &context, IRCatalog &catalog, IRCSchemaEntry &schema, CreateTableInfo &info);

public:
	void LoadEntries(ClientContext &context);
	void FillEntry(ClientContext &context, IcebergTableInformation &table);

public:
	IRCSchemaEntry &schema;
	Catalog &catalog;
	case_insensitive_map_t<IcebergTableInformation> entries;

private:
	mutex entry_lock;
};

} // namespace duckdb
