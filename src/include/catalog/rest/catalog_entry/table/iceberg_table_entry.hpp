
#pragma once

#include "catalog/rest/api/catalog_api.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

struct IcebergTableInformation;
struct IcebergTransactionData;
struct IcebergTransactionTableState;

class IcebergTableEntry : public TableCatalogEntry {
public:
	IcebergTableEntry(IcebergTableInformation &table_info, Catalog &catalog, SchemaCatalogEntry &schema,
	                  CreateTableInfo &info, optional_idx schema_id,
	                  optional_ptr<IcebergTransactionTableState> transaction_state = nullptr);

	static virtual_column_map_t VirtualColumns();
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;
	const IcebergTableMetadata &GetTableMetadata() const;
	optional_ptr<IcebergTransactionData> GetTransactionData() const;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	void PrepareIcebergScanFromEntry(ClientContext &context) const;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	LogicalType GetExpectedTypeForInsert(const ColumnDefinition &column) const override;
	unique_ptr<Expression> GetDefaultExpressionForColumn(ClientContext &context, const LogicalType &input_type,
	                                                     const LogicalType &result_type, ColumnBinding binding,
	                                                     const Expression &constant_value) const override;

	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;
	string GetUUID() const;

public:
	IcebergTableInformation &table_info;
	//! 'schema_id' used to create the entry, or invalid if this is a dummy
	optional_idx schema_id;
	optional_ptr<IcebergTransactionTableState> transaction_state;
};

struct IcebergTableEntryHashFunction {
	uint64_t operator()(const optional_ptr<IcebergTableEntry> &entry) const {
		D_ASSERT(entry);
		auto table_uuid = entry->GetUUID();
		return std::hash<string>()(table_uuid);
	}
};

} // namespace duckdb
