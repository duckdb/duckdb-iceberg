
#pragma once

#include "storage/irc_catalog_set.hpp"
#include "storage/irc_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class ICResult;
class IRCSchemaEntry;

class ICInSchemaSet : public IRCCatalogSet {
public:
	ICInSchemaSet(IRCSchemaEntry &schema);

	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry) override;

protected:
	IRCSchemaEntry &schema;
};

class ICTableSet : public ICInSchemaSet {
public:
	explicit ICTableSet(IRCSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);
	static unique_ptr<ICTableInfo> GetTableInfo(ClientContext &context, IRCSchemaEntry &schema,
	                                            const string &table_name);
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
	unique_ptr<CatalogEntry> _CreateCatalogEntry(ClientContext &context, IRCAPITable &&table);
};

} // namespace duckdb
