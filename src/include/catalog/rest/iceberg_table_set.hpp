
#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"

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
	static IcebergTableInformation &CreateNewEntry(ClientContext &context, IcebergCatalog &catalog,
	                                               IcebergSchemaEntry &schema, CreateTableInfo &info);
	const case_insensitive_map_t<IcebergTableInformation> &GetEntries();
	case_insensitive_map_t<IcebergTableInformation> &GetEntriesMutable();

public:
	void LoadEntries(ClientContext &context);
	//! return true if request to LoadTableInformation was successful and entry has been filled
	//! or if entry is already filled. Returns False otherwise
	bool FillEntry(ClientContext &context, IcebergTableInformation &table);

	//! View operations
	optional_ptr<CatalogEntry> GetViewEntry(ClientContext &context, const string &view_name);
	void LoadViewEntries(ClientContext &context);
	void ScanViews(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);

	const case_insensitive_map_t<unique_ptr<CreateViewInfo>> &GetViewEntries() const;
	case_insensitive_map_t<unique_ptr<CreateViewInfo>> &GetViewEntriesMutable();
	void InvalidateViewCache(const string &view_name);

public:
	IcebergSchemaEntry &schema;
	Catalog &catalog;

private:
	case_insensitive_map_t<IcebergTableInformation> entries;
	case_insensitive_map_t<unique_ptr<CreateViewInfo>> view_entries;
	//! Cached ViewCatalogEntry instances for Scan
	case_insensitive_map_t<unique_ptr<ViewCatalogEntry>> view_catalog_entries;
	mutex entry_lock;
};

} // namespace duckdb
