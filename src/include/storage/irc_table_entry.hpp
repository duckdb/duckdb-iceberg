
#pragma once

#include "catalog_api.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

struct IcebergTableInformation;

struct ICTableInfo {
	ICTableInfo() {
		create_info = make_uniq<CreateTableInfo>();
	}
	ICTableInfo(const string &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>(string(), schema, table);
	}
	ICTableInfo(const SchemaCatalogEntry &schema, const string &table) {
		create_info = make_uniq<CreateTableInfo>((SchemaCatalogEntry &)schema, table);
	}

	const string &GetTableName() const {
		return create_info->table;
	}

	unique_ptr<CreateTableInfo> create_info;
};

class ICTableEntry : public TableCatalogEntry {
public:
	ICTableEntry(IcebergTableInformation &table_info, Catalog &catalog, SchemaCatalogEntry &schema,
	             CreateTableInfo &info);

	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	string PrepareIcebergScanFromEntry(ClientContext &context) const;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

public:
	IcebergTableInformation &table_info;
};

} // namespace duckdb
