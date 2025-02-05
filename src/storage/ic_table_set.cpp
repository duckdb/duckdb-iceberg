#include "catalog_api.hpp"
#include "catalog_utils.hpp"

#include "storage/ic_catalog.hpp"
#include "storage/ic_table_set.hpp"
#include "storage/ic_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/ic_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

ICTableSet::ICTableSet(ICSchemaEntry &schema) : ICInSchemaSet(schema) {
}

static ColumnDefinition CreateColumnDefinition(ClientContext &context, ICAPIColumnDefinition &coldef) {
	return {coldef.name, ICUtils::TypeToLogicalType(context, coldef.type_text)};
}

unique_ptr<CatalogEntry> ICTableSet::CreateTableEntry(ClientContext &context, ICAPITable table) {
	D_ASSERT(schema.name == table.schema_name);
	CreateTableInfo info;
	info.table = table.name;

	for (auto &col : table.columns) {
		info.columns.AddColumn(CreateColumnDefinition(context, col));
	}

	auto table_entry = make_uniq<ICTableEntry>(catalog, schema, info);
	table_entry->table_data = make_uniq<ICAPITable>(table);
	return table_entry;
}

void ICTableSet::FillEntry(ClientContext &context, unique_ptr<CatalogEntry> &entry) {
	auto* derived = static_cast<ICTableEntry*>(entry.get());
	if (!derived->table_data->storage_location.empty()) {
		return;
	}
		
	auto &ic_catalog = catalog.Cast<ICCatalog>();
	auto table = ICAPI::GetTable(catalog.GetName(), catalog.GetDBPath(), schema.name, entry->name, ic_catalog.credentials);
	entry = CreateTableEntry(context, table);
}

void ICTableSet::LoadEntries(ClientContext &context) {
	if (!entries.empty()) {
		return;
	}

	auto &ic_catalog = catalog.Cast<ICCatalog>();
	// TODO: handle out-of-order columns using position property
	auto tables = ICAPI::GetTables(catalog.GetName(), catalog.GetDBPath(), schema.name, ic_catalog.credentials);

	for (auto &table : tables) {
		auto entry = CreateTableEntry(context, table);
		AddEntry(std::move(entry));
	}
}

optional_ptr<CatalogEntry> ICTableSet::RefreshTable(ClientContext &context, const string &table_name) {
	auto table_info = GetTableInfo(context, schema, table_name);
	auto table_entry = make_uniq<ICTableEntry>(catalog, schema, *table_info);
	auto table_ptr = table_entry.get();
	AddEntry(std::move(table_entry));
	return table_ptr;
}

unique_ptr<ICTableInfo> ICTableSet::GetTableInfo(ClientContext &context, ICSchemaEntry &schema,
                                                 const string &table_name) {
	throw NotImplementedException("ICTableSet::GetTableInfo");
}

optional_ptr<CatalogEntry> ICTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
	auto &ic_catalog = catalog.Cast<ICCatalog>();
	auto *table_info = dynamic_cast<CreateTableInfo *>(info.base.get());
	auto table = ICAPI::CreateTable(catalog.GetName(), ic_catalog.internal_name, schema.name, ic_catalog.credentials, table_info);
	auto entry = CreateTableEntry(context, table);
	return AddEntry(std::move(entry));
}

void ICTableSet::DropTable(ClientContext &context, DropInfo &info) {
	auto &ic_catalog = catalog.Cast<ICCatalog>();
	ICAPI::DropTable(catalog.GetName(), ic_catalog.internal_name, schema.name, info.name, ic_catalog.credentials);	
}

void ICTableSet::AlterTable(ClientContext &context, RenameTableInfo &info) {
	throw NotImplementedException("ICTableSet::AlterTable");
}

void ICTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info) {
	throw NotImplementedException("ICTableSet::AlterTable");
}

void ICTableSet::AlterTable(ClientContext &context, AddColumnInfo &info) {
	throw NotImplementedException("ICTableSet::AlterTable");
}

void ICTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info) {
	throw NotImplementedException("ICTableSet::AlterTable");
}

void ICTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter) {
	throw NotImplementedException("ICTableSet::AlterTable");
}

ICInSchemaSet::ICInSchemaSet(ICSchemaEntry &schema) : ICCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> ICInSchemaSet::AddEntry(unique_ptr<CatalogEntry> entry) {
	std::cout << "ICInSchemaSet::CreateEntry" << std::endl;
	
	if (!entry->internal) {
		entry->internal = schema.internal;
	}
	return ICCatalogSet::AddEntry(std::move(entry));
}

} // namespace duckdb
