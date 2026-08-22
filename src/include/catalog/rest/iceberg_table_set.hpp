
#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread_annotation.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "catalog/rest/case_aware_identifier_container.hpp"

namespace duckdb {
struct CreateTableInfo;
struct DropInfo;
class IcebergSchemaEntry;
class IcebergTransaction;

class IcebergTableSet {
public:
	explicit IcebergTableSet(IcebergSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	void ScanTables(ClientContext &context, const std::function<void(IcebergTable &)> &callback);
	void DropEntry(ClientContext &context, DropInfo &info, bool delete_entry);
	void RenameEntry(const string &name, const string &new_name, IcebergTable &&new_table);
	static IcebergTable &CreateNewEntry(ClientContext &context, IcebergCatalog &catalog, IcebergSchemaEntry &schema,
	                                    CreateTableInfo &info);

private:
	IcebergTableSchemaVersion &GetOrCreateDummy(IcebergTable &table_info) const DUCKDB_REQUIRES(entry_lock);
	void LoadEntriesInternal(ClientContext &context) DUCKDB_REQUIRES(entry_lock);
	shared_ptr<IcebergTable> CreateEntryInternal(const string &name, IcebergTable &&table,
	                                             shared_ptr<IcebergTable> &old_entry) DUCKDB_REQUIRES(entry_lock);

public:
	//! return true if request to LoadTableInformation was successful and entry has been filled
	//! or if entry is already filled. Returns False otherwise
	bool FillEntry(ClientContext &context, IcebergTable &table);

private:
	//! Resolves the canonical (server-cased) table name for a case-insensitive cold lookup by
	//! listing tables in this schema and matching case-insensitively. Returns the canonical
	//! name, or an empty string if no table matches. Throws on an ambiguous (multiple
	//! case-fold) match.
	string ResolveCanonicalNameViaList(ClientContext &context, const string &name);

public:
	IcebergSchemaEntry &schema;
	Catalog &catalog;
	const CaseSensitivityMode mode;

private:
	annotated_mutex entry_lock;
	CaseAwareIdentifierMap<shared_ptr<IcebergTable>> entries DUCKDB_GUARDED_BY(entry_lock);
};

} // namespace duckdb
