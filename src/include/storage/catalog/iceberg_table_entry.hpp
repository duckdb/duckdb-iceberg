
#pragma once

#include "catalog_api.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

struct IcebergTableInformation;

class IcebergTableEntry : public TableCatalogEntry {
public:
	IcebergTableEntry(IcebergTableInformation &table_info, Catalog &catalog, SchemaCatalogEntry &schema,
	                  CreateTableInfo &info);

	static virtual_column_map_t VirtualColumns();
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	void PrepareIcebergScanFromEntry(ClientContext &context) const;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;
	string GetUUID() const;

public:
	IcebergTableInformation &table_info;
};

struct IcebergTableEntryHashFunction {
	uint64_t operator()(const optional_ptr<IcebergTableEntry> &entry) const {
		D_ASSERT(entry);
		auto table_uuid = entry->GetUUID();
		return std::hash<string>()(table_uuid);
	}
};

} // namespace duckdb
