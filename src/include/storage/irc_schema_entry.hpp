
#pragma once

#include "catalog_api.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "storage/irc_table_set.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"

namespace duckdb {
class IRCTransaction;
struct IRCAPISchema;

class IRCSchemaEntry : public SchemaCatalogEntry {
public:
	IRCSchemaEntry(Catalog &catalog, CreateSchemaInfo &info);
	~IRCSchemaEntry() override;

	//! The various levels of namespaces this flattened representation represents
	vector<string> namespace_items;

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction &transaction, ClientContext &context,
	                                       BoundCreateTableInfo &info);
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;
	bool HandleCreateConflict(CatalogTransaction &transaction, CatalogType catalog_type, const string &entry_name,
	                          OnCreateConflict on_conflict);

private:
	ICTableSet &GetCatalogSet(CatalogType type);

public:
	ICTableSet tables;
};

} // namespace duckdb
