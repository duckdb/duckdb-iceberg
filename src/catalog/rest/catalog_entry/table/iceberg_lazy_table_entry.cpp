#include "catalog/rest/catalog_entry/table/iceberg_lazy_table_entry.hpp"

#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_table_set.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

IcebergLazyTableEntry::IcebergLazyTableEntry(IcebergTableSet &table_set, IcebergTransaction &transaction,
                                             CreateTableInfo &info, string table_key_p)
    : TableCatalogEntry(table_set.catalog, table_set.schema, info), table_set(table_set), transaction(transaction),
      table_key(std::move(table_key_p)) {
	internal = table_set.schema.internal;
}

optional_ptr<TableCatalogEntry> IcebergLazyTableEntry::Resolve() const {
	// Listings are read on execution threads, possibly by several pipelines at once
	lock_guard<std::recursive_mutex> guard(transaction.table_lookup_lock);
	auto state = transaction.GetLatestTableState(table_key);
	if (state) {
		if (!state->IsAlive()) {
			return nullptr;
		}
		// Already resolved by this transaction: avoid the full lookup, a listing reads its columns many times
		auto &table = state->GetInfo();
		if (!table.schema_versions.empty()) {
			auto latest = table.GetLatestSchema();
			if (!latest) {
				return nullptr;
			}
			return &latest->Cast<TableCatalogEntry>();
		}
	}
	if (load_failed) {
		return nullptr;
	}
	auto context = transaction.context.lock();
	if (!context) {
		throw InternalException("Iceberg table '%s' was read from a listing after its transaction ended", table_key);
	}
	optional_ptr<CatalogEntry> entry;
	try {
		DUCKDB_LOG(*context, IcebergLogType, "Loading Iceberg table '%s' for a catalog listing", table_key);
		// The lookup a query uses: it loads the table (or takes it from a cache or prefetch) and pins it in the
		// transaction, so later queries of this transaction see the same version without loading it again
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(name));
		entry = table_set.GetEntry(*context, lookup);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		// An interrupted request can surface as a different (e.g. HTTP) error
		context->InterruptCheck();
		if (error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::INTERNAL ||
		    error.Type() == ExceptionType::FATAL) {
			throw;
		}
		// A table that can not be loaded, e.g. a non-Iceberg table listed by Glue, must not fail the whole listing.
		// Looking the table up directly still reports the error.
		load_failed = true;
		DUCKDB_LOG_WARNING(*context, "Could not resolve the columns of Iceberg table '%s' while listing: %s", table_key,
		                   error.RawMessage());
		return nullptr;
	}
	if (!entry) {
		// Dropped since it was listed
		return nullptr;
	}
	return &entry->Cast<TableCatalogEntry>();
}

void IcebergLazyTableEntry::SetLoadFailed() {
	load_failed = true;
}

const ColumnList &IcebergLazyTableEntry::GetColumns() const {
	auto resolved = Resolve();
	return resolved ? resolved->GetColumns() : no_columns;
}

const vector<unique_ptr<Constraint>> &IcebergLazyTableEntry::GetConstraints() const {
	auto resolved = Resolve();
	return resolved ? resolved->GetConstraints() : constraints;
}

unique_ptr<CreateInfo> IcebergLazyTableEntry::GetInfo() const {
	auto resolved = Resolve();
	if (resolved) {
		return resolved->GetInfo();
	}
	return TableCatalogEntry::GetInfo();
}

unique_ptr<BaseStatistics> IcebergLazyTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableStorageInfo IcebergLazyTableEntry::GetStorageInfo(ClientContext &context) {
	return TableStorageInfo();
}

TableFunction IcebergLazyTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	throw InternalException("GetScanFunction was called on the listing entry of Iceberg table '%s'", table_key);
}

TableFunction IcebergLazyTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                     const EntryLookupInfo &lookup) {
	throw InternalException("GetScanFunction was called on the listing entry of Iceberg table '%s'", table_key);
}

} // namespace duckdb
