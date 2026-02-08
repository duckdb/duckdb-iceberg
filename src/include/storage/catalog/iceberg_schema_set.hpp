
#pragma once

#include "iceberg_schema_entry.hpp"
#include "duckdb/common/string.hpp"

#include "duckdb/common/unique_ptr.hpp"
#include "storage/catalog/iceberg_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class IcebergSchemaSet {
public:
	explicit IcebergSchemaSet(Catalog &catalog);

public:
	void LoadEntries(ClientContext &context);
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name, OnEntryNotFound if_not_found);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	const case_insensitive_map_t<unique_ptr<CatalogEntry>> &GetEntries();
	void AddEntry(const string &name, unique_ptr<IcebergSchemaEntry> entry);
	void RemoveEntry(const string &name);
	CatalogEntry &GetEntry(const string &name);

protected:
	optional_ptr<CatalogEntry> CreateEntryInternal(ClientContext &context, unique_ptr<CatalogEntry> entry);

public:
	Catalog &catalog;

private:
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;
	mutex entry_lock;
};

} // namespace duckdb
