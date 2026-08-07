
#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/vector.hpp"

#include "catalog_entry/schema/iceberg_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class IcebergSchemaSet {
public:
	explicit IcebergSchemaSet(Catalog &catalog);

public:
	void LoadEntries(ClientContext &context);
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name, OnEntryNotFound if_not_found);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	vector<shared_ptr<IcebergSchemaEntry>> GetEntries(ClientContext &context);
	void AddEntry(const string &name, shared_ptr<IcebergSchemaEntry> entry);
	void RemoveEntry(const string &name);

protected:
	void LoadEntriesInternal(ClientContext &context) DUCKDB_REQUIRES(entry_lock);
	shared_ptr<IcebergSchemaEntry> CreateEntryInternal(shared_ptr<IcebergSchemaEntry> entry)
	    DUCKDB_REQUIRES(entry_lock);

public:
	Catalog &catalog;

private:
	annotated_mutex entry_lock;
	case_insensitive_map_t<shared_ptr<IcebergSchemaEntry>> entries DUCKDB_GUARDED_BY(entry_lock);
};

} // namespace duckdb
