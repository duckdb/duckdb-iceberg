//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/catalog_entry/table/iceberg_lazy_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class IcebergTableSet;
class IcebergTransaction;

//! The entry a table listing returns for a table the transaction has not resolved yet. Listing only costs the
//! ListTables request: the table is loaded the first time its metadata (columns, constraints, SQL) is read, through
//! the same lookup a query uses, so a listing reports exactly the table version the transaction reads.
//! It holds no metadata of its own and forwards to the transaction-visible schema version on every access, so
//! changes made later in the transaction stay visible. Owned by the transaction, which hands out the same entry for
//! a table on every listing, keeping its OID stable (SHOW ALL TABLES joins on it).
class IcebergLazyTableEntry : public TableCatalogEntry {
public:
	IcebergLazyTableEntry(IcebergTableSet &table_set, IcebergTransaction &transaction, CreateTableInfo &info,
	                      string table_key);

public:
	//! The listing already failed (and warned) to load the table: report it without columns instead of retrying.
	//! Requires the transaction's table_lookup_lock.
	void SetLoadFailed();

	const ColumnList &GetColumns() const override;
	const vector<unique_ptr<Constraint>> &GetConstraints() const override;
	unique_ptr<CreateInfo> GetInfo() const override;

	//! Neither needs the table: Iceberg entries report no statistics or storage info, so do not load for them
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	//! Binding looks tables up through the catalog, never through a listing entry
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup) override;

private:
	//! The schema version a query in this transaction would bind, or nullptr if the table no longer exists or could
	//! not be loaded
	optional_ptr<TableCatalogEntry> Resolve() const;

private:
	IcebergTableSet &table_set;
	IcebergTransaction &transaction;
	const string table_key;
	//! Reported while the table can not be resolved
	const ColumnList no_columns;
	//! Set when loading the table failed: the failure is reported once, not once per column read by a listing.
	//! Guarded by the transaction's table_lookup_lock.
	mutable bool load_failed = false;
};

} // namespace duckdb
