
#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "storage/catalog/iceberg_table_entry.hpp"
#include "storage/iceberg_table_information.hpp"
#include "storage/iceberg_transaction_data.hpp"

namespace duckdb {
struct CreateTableInfo;
class IcebergSchemaEntry;
class IcebergTransaction;

class IcebergTableSet {
public:
	explicit IcebergTableSet(IcebergSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	static bool CreateNewEntry(ClientContext &context, IcebergCatalog &catalog, IcebergSchemaEntry &schema,
	                           CreateTableInfo &info);
	const case_insensitive_map_t<IcebergTableInformation> &GetEntries();
	case_insensitive_map_t<IcebergTableInformation> &GetEntriesMutable();

public:
	void LoadEntries(ClientContext &context);
	//! return true if request to LoadTableInformation was successful and entry has been filled
	//! or if entry is already filled. Returns False otherwise
	bool FillEntry(ClientContext &context, IcebergTableInformation &table);

public:
	IcebergSchemaEntry &schema;
	Catalog &catalog;

private:
	case_insensitive_map_t<IcebergTableInformation> entries;
	mutex entry_lock;
};

} // namespace duckdb
