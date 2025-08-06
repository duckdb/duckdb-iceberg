#include "storage/irc_schema_entry.hpp"

#include "storage/irc_table_entry.hpp"
#include "storage/irc_transaction.hpp"
#include "utils/iceberg_type.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "storage/irc_catalog.hpp"
namespace duckdb {

IRCSchemaEntry::IRCSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this) {
}

IRCSchemaEntry::~IRCSchemaEntry() {
}

IRCTransaction &GetICTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<IRCTransaction>();
}

optional_ptr<CatalogEntry> IRCSchemaEntry::CreateTable(IRCTransaction &irc_transaction, ClientContext &context,
                                                       BoundCreateTableInfo &info) {
	auto &base_info = info.Base();

	auto &catalog = irc_transaction.GetCatalog();

	// always posts to IRC catalog so we can get the metadata
	tables.CreateNewEntry(context, catalog, *this, base_info);
	auto lookup_info = EntryLookupInfo(CatalogType::TABLE_ENTRY, base_info.table);
	auto entry = tables.GetEntry(context, lookup_info);
	auto &ic_entry = entry->Cast<ICTableEntry>();
	// An eagerly created table (if stage_create isn't supported by Catalog) is not marked as dirty, because it requires
	// no action on commit/abort. We do not drop it on abort because that isn't transactionally safe (no guarantees a
	// different transaction didn't interact with the created table in the meantime)
	if (catalog.attach_options.supports_stage_create) {
		// mark table as dirty so at the end of the transaction updates/creates are commited
		irc_transaction.MarkTableAsDirty(ic_entry);
	}

	// get the entry from the catalog.
	D_ASSERT(entry);
	D_ASSERT(entry->type == CatalogType::TABLE_ENTRY);

	return entry;
}

optional_ptr<CatalogEntry> IRCSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &irc_transaction = transaction.transaction->Cast<IRCTransaction>();
	auto &context = transaction.context;
	// directly create the table with stage_create = true;
	return CreateTable(irc_transaction, *context, info);
}

void IRCSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	//	auto &ic_catalog = catalog.Cast<IRCatalog>();
	auto &transaction = IRCTransaction::Get(context, catalog);
	auto table_name = info.name;
	// find if info has a table name, if so look for it in
	auto table_info_it = tables.entries.find(table_name);
	if (table_info_it == tables.entries.end()) {
		throw InvalidInputException("Table %s does not exist");
	}
	auto &table_entry = table_info_it->second;
	auto lookupInfo = EntryLookupInfo(CatalogType::TABLE_ENTRY, table_name);
	auto catalog_transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto table_entry_actual = LookupEntry(catalog_transaction, lookupInfo);
	auto &ic_entry = table_entry_actual->Cast<ICTableEntry>();
	D_ASSERT(table_entry_actual);
	if (!table_entry.transaction_data) {
		table_entry.transaction_data = make_uniq<IcebergTransactionData>(context, table_entry);
	}
	table_entry.transaction_data->is_deleted = true;
	transaction.MarkTableAsDeleted(ic_entry);
}

optional_ptr<CatalogEntry> IRCSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
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

optional_ptr<CatalogEntry> IRCSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                       TableCatalogEntry &table) {
	throw NotImplementedException("Create Index");
}

string GetUCCreateView(CreateViewInfo &info) {
	throw NotImplementedException("Get Create View");
}

optional_ptr<CatalogEntry> IRCSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("Create View");
}

optional_ptr<CatalogEntry> IRCSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("Iceberg databases do not support creating types");
}

optional_ptr<CatalogEntry> IRCSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("Iceberg databases do not support creating sequences");
}

optional_ptr<CatalogEntry> IRCSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                               CreateTableFunctionInfo &info) {
	throw BinderException("Iceberg databases do not support creating table functions");
}

optional_ptr<CatalogEntry> IRCSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                              CreateCopyFunctionInfo &info) {
	throw BinderException("Iceberg databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> IRCSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                CreatePragmaFunctionInfo &info) {
	throw BinderException("Iceberg databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> IRCSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw BinderException("Iceberg databases do not support creating collations");
}

void IRCSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
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

void IRCSchemaEntry::Scan(ClientContext &context, CatalogType type,
                          const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	GetCatalogSet(type).Scan(context, callback);
}
void IRCSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

optional_ptr<CatalogEntry> IRCSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                       const EntryLookupInfo &lookup_info) {
	auto type = lookup_info.GetCatalogType();
	if (!CatalogTypeIsSupported(type)) {
		return nullptr;
	}
	return GetCatalogSet(type).GetEntry(transaction.GetContext(), lookup_info);
}

ICTableSet &IRCSchemaEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables;
	default:
		throw InternalException("Type not supported for GetCatalogSet");
	}
}

} // namespace duckdb
