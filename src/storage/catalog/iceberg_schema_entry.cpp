#include "storage/catalog/iceberg_schema_entry.hpp"

#include "storage/iceberg_table_information.hpp"
#include "storage/catalog/iceberg_catalog.hpp"
#include "storage/iceberg_transaction.hpp"
#include "storage/catalog/iceberg_table_entry.hpp"
#include "storage/iceberg_transaction.hpp"
#include "utils/iceberg_type.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "storage/catalog/iceberg_catalog.hpp"
namespace duckdb {

IcebergSchemaEntry::IcebergSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), namespace_items(IRCAPI::ParseSchemaName(info.schema)), tables(*this) {
}

IcebergSchemaEntry::~IcebergSchemaEntry() {
}

IcebergTransaction &GetICTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<IcebergTransaction>();
}

bool IcebergSchemaEntry::HandleCreateConflict(CatalogTransaction &transaction, CatalogType catalog_type,
                                              const string &entry_name, OnCreateConflict on_conflict) {
	auto existing_entry = GetEntry(transaction, catalog_type, entry_name);
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException(
		    "CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements");
	}
	if (!existing_entry) {
		// If there is no existing entry, make sure the entry has not been deleted in this transaction.
		// We cannot create (or stage create) a table replace within a transaction yet.
		// FIXME: With Snapshot operation type overwrite, you can handle create or replace for tables.
		auto &iceberg_transaction = GetICTransaction(transaction);
		auto table_key = IcebergTableInformation::GetTableKey(namespace_items, entry_name);
		auto deleted_table_entry = iceberg_transaction.deleted_tables.find(table_key);
		if (deleted_table_entry != iceberg_transaction.deleted_tables.end()) {
			auto &ic_catalog = catalog.Cast<IcebergCatalog>();
			vector<string> qualified_name = {ic_catalog.GetName()};
			qualified_name.insert(qualified_name.end(), namespace_items.begin(), namespace_items.end());
			qualified_name.push_back(entry_name);
			auto qualified_table_name = StringUtil::Join(qualified_name, ".");
			throw NotImplementedException("Cannot create table deleted within a transaction: %s", qualified_table_name);
		}
		// no conflict
		return true;
	}
	switch (on_conflict) {
	case OnCreateConflict::ERROR_ON_CONFLICT:
		throw CatalogException("%s with name \"%s\" already exists", CatalogTypeToString(existing_entry->type),
		                       entry_name);
	case OnCreateConflict::IGNORE_ON_CONFLICT: {
		// ignore - skip without throwing an error
		return false;
	}
	default:
		throw InternalException("DuckDB-Iceberg, Unsupported conflict type: %s", EnumUtil::ToString(on_conflict));
	}
	return true;
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateTable(CatalogTransaction &transaction, ClientContext &context,
                                                           BoundCreateTableInfo &info) {
	auto &base_info = info.Base();
	auto &ir_catalog = catalog.Cast<IcebergCatalog>();
	auto &iceberg_transaction = GetICTransaction(transaction);
	// check if we have an existing entry with this name
	if (!HandleCreateConflict(transaction, CatalogType::TABLE_ENTRY, base_info.table, base_info.on_conflict)) {
		return nullptr;
	}

	if (!IcebergTableSet::CreateNewEntry(context, ir_catalog, *this, base_info)) {
		throw InternalException("Could not create entry %s", base_info.table);
	}
	auto table_key = IcebergTableInformation::GetTableKey(namespace_items, base_info.table);
	D_ASSERT(iceberg_transaction.updated_tables.count(table_key) > 0);
	auto entry = iceberg_transaction.updated_tables.find(table_key);
	return entry->second.schema_versions[0].get();
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &context = transaction.context;
	// directly create the table with stage_create = true;
	return CreateTable(transaction, *context, info);
}

void IcebergSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	DropEntry(context, info, false);
}

void IcebergSchemaEntry::DropEntry(ClientContext &context, DropInfo &info, bool delete_entry) {
	auto table_name = info.name;
	// find if info has a table name, if so look for it in
	auto table_info_it = tables.GetEntries().find(table_name);
	if (table_info_it == tables.GetEntries().end()) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Table %s does not exist");
	}
	if (info.cascade) {
		throw NotImplementedException("DROP TABLE <table_name> CASCADE is not supported for Iceberg tables currently");
	}
	if (delete_entry) {
		// Remove the entry from the catalog
		tables.GetEntriesMutable().erase(table_name);
	} else {
		// Add the table to the transaction's deleted_tables
		auto &transaction = IcebergTransaction::Get(context, catalog).Cast<IcebergTransaction>();
		auto &table_info = table_info_it->second;
		auto table_key = table_info.GetTableKey();
		transaction.deleted_tables.emplace(table_key, table_info.Copy());
		D_ASSERT(transaction.deleted_tables.count(table_key) > 0);
		auto &deleted_table_info = transaction.deleted_tables.at(table_key);
		// must init schema versions after copy. Schema versions have a pointer to IcebergTableInformation
		// if the IcebergTableInformation is moved, then the pointer is no longer valid.
		deleted_table_info.InitSchemaVersions();
	}
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                              CreateFunctionInfo &info) {
	throw BinderException("Iceberg databases do not support creating functions");
}

void ICUnqualifyColumnRef(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto name = std::move(colref.column_names.back());
		colref.column_names = {std::move(name)};
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr, ICUnqualifyColumnRef);
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                           TableCatalogEntry &table) {
	throw NotImplementedException("Create Index");
}

string GetUCCreateView(CreateViewInfo &info) {
	throw NotImplementedException("Get Create View");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("Create View");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("Iceberg databases do not support creating types");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                              CreateSequenceInfo &info) {
	throw BinderException("Iceberg databases do not support creating sequences");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                   CreateTableFunctionInfo &info) {
	throw BinderException("Iceberg databases do not support creating table functions");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                  CreateCopyFunctionInfo &info) {
	throw BinderException("Iceberg databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                    CreatePragmaFunctionInfo &info) {
	throw BinderException("Iceberg databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                               CreateCollationInfo &info) {
	throw BinderException("Iceberg databases do not support creating collations");
}

void IcebergSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("Alter Schema Entry");
}

static bool CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return true;
	default:
		return false;
	}
}

void IcebergSchemaEntry::Scan(ClientContext &context, CatalogType type,
                              const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	GetCatalogSet(type).Scan(context, callback);
}
void IcebergSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                           const EntryLookupInfo &lookup_info) {
	auto type = lookup_info.GetCatalogType();
	if (!CatalogTypeIsSupported(type)) {
		return nullptr;
	}
	return GetCatalogSet(type).GetEntry(transaction.GetContext(), lookup_info);
}

IcebergTableSet &IcebergSchemaEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables;
	default:
		throw InternalException("Type not supported for GetCatalogSet");
	}
}

} // namespace duckdb
