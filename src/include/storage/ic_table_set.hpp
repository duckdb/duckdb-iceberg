
#pragma once

#include "storage/ic_catalog_set.hpp"
#include "storage/ic_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class ICResult;
class ICSchemaEntry;


class ICInSchemaSet : public ICCatalogSet {
public:
	ICInSchemaSet(ICSchemaEntry &schema);

	optional_ptr<CatalogEntry> AddEntry(unique_ptr<CatalogEntry> entry) override;

protected:
	ICSchemaEntry &schema;
};


class ICTableSet : public ICInSchemaSet {
public:
	explicit ICTableSet(ICSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);
	static unique_ptr<ICTableInfo> GetTableInfo(ClientContext &context, ICSchemaEntry &schema, const string &table_name);
	optional_ptr<CatalogEntry> RefreshTable(ClientContext &context, const string &table_name);
	void AlterTable(ClientContext &context, AlterTableInfo &info);
	void DropTable(ClientContext &context, DropInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;
	void FillEntry(ClientContext &context, unique_ptr<CatalogEntry> &entry) override;

	void AlterTable(ClientContext &context, RenameTableInfo &info);
	void AlterTable(ClientContext &context, RenameColumnInfo &info);
	void AlterTable(ClientContext &context, AddColumnInfo &info);
	void AlterTable(ClientContext &context, RemoveColumnInfo &info);

	static void AddColumn(ClientContext &context, ICResult &result, ICTableInfo &table_info, idx_t column_offset = 0);

private:
	unique_ptr<CatalogEntry> CreateTableEntry(ClientContext &context, ICAPITable table);
};


} // namespace duckdb
