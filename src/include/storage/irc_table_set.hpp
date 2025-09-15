
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
	bool CreateNewEntry(ClientContext &context, IRCatalog &catalog, IRCSchemaEntry &schema, CreateTableInfo &info);

public:
	void LoadEntries(ClientContext &context);
	//! return true if request to LoadTableInformation was successful and entry has been filled
	//! or if entry is already filled. Returns False otherwise
	bool FillEntry(ClientContext &context, IcebergTableInformation &table);

public:
	IRCSchemaEntry &schema;
	Catalog &catalog;
	case_insensitive_map_t<IcebergTableInformation> entries;
	//! Whether a listing is done for this transaction
	bool listed = false;

private:
	mutex entry_lock;
};

} // namespace duckdb
