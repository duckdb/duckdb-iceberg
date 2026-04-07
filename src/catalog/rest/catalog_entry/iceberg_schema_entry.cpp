#include "catalog/rest/catalog_entry/iceberg_schema_entry.hpp"

#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/api/iceberg_type.hpp"

namespace duckdb {

IcebergSchemaEntry::IcebergSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), namespace_items(IRCAPI::ParseSchemaName(info.schema)), exists(true),
      tables(*this) {
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
	// check if we have an existing entry with this name
	if (!HandleCreateConflict(transaction, CatalogType::TABLE_ENTRY, base_info.table, base_info.on_conflict)) {
		return nullptr;
	}

	auto &table_info = IcebergTableSet::CreateNewEntry(context, ir_catalog, *this, base_info);
	return table_info.schema_versions[0].get();
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
	auto entry_name = info.name;

	// Handle VIEW_ENTRY drops
	if (info.type == CatalogType::VIEW_ENTRY) {
		auto &transaction = IcebergTransaction::Get(context, catalog).Cast<IcebergTransaction>();
		auto view_key = IcebergTableInformation::GetTableKey(namespace_items, entry_name);

		// Check if view was created in this transaction — just remove from created_views
		if (transaction.created_views.erase(view_key) > 0) {
			tables.InvalidateViewCache(entry_name);
			return;
		}

		// Load view entries if not yet loaded, then check if view exists
		tables.LoadViewEntries(context);
		auto view_it = tables.GetViewEntries().find(entry_name);
		if (view_it == tables.GetViewEntries().end()) {
			if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
				return;
			}
			throw CatalogException("View %s does not exist", entry_name);
		}

		if (delete_entry) {
			tables.InvalidateViewCache(entry_name);
		} else {
			IcebergTransaction::DeletedViewInfo info;
			info.namespace_items = namespace_items;
			info.schema_name = name;
			info.view_name = entry_name;
			transaction.deleted_views.emplace(view_key, std::move(info));
		}
		return;
	}

	// Handle TABLE_ENTRY drops (existing logic)
	auto table_info_it = tables.GetEntries().find(entry_name);
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
		tables.GetEntriesMutable().erase(entry_name);
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

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	if (info.sql.empty() && !info.query) {
		throw BinderException("Cannot create view in Iceberg without a query");
	}
	auto &context = transaction.GetContext();

	// Handle CREATE OR REPLACE and CREATE IF NOT EXISTS
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT ||
	    info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		auto existing_entry = GetEntry(transaction, CatalogType::VIEW_ENTRY, info.view_name);
		if (existing_entry) {
			if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
				return existing_entry;
			}
			// CREATE OR REPLACE — drop first, then create
			DropInfo drop_info;
			drop_info.type = CatalogType::VIEW_ENTRY;
			drop_info.name = info.view_name;
			drop_info.cascade = false;
			drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
			DropEntry(context, drop_info, false);
		}
	} else {
		// ERROR_ON_CONFLICT — check existence
		auto existing_entry = GetEntry(transaction, CatalogType::VIEW_ENTRY, info.view_name);
		if (existing_entry) {
			throw CatalogException("View with name \"%s\" already exists", info.view_name);
		}
	}

	// Ensure names are populated — the binder populates types+names on the original info,
	// but as a safety net, fill them in if missing.
	if (info.types.empty() && info.query) {
		auto copy = unique_ptr_cast<CreateInfo, CreateViewInfo>(info.Copy());
		auto bound = CreateViewInfo::FromSelect(transaction.GetContext(), std::move(copy));
		info.types = bound->types;
		info.names = bound->names;
	} else if (info.names.empty() && !info.types.empty()) {
		for (idx_t i = 0; i < info.types.size(); i++) {
			if (i < info.aliases.size() && !info.aliases[i].empty()) {
				info.names.push_back(info.aliases[i]);
			} else {
				info.names.push_back("col" + to_string(i));
			}
		}
	}

	// Track the view in the transaction
	auto &iceberg_transaction = GetICTransaction(transaction);
	auto view_key = IcebergTableInformation::GetTableKey(namespace_items, info.view_name);

	// Workaround: CreateViewInfo::Copy() does not copy `names`, so we patch it manually.
	// See: https://github.com/duckdb/duckdb/pull/21817
	auto view_info = unique_ptr_cast<CreateInfo, CreateViewInfo>(info.Copy());
	view_info->names = info.names;
	// Preserve the SELECT SQL — ViewCatalogEntry::Initialize() will move the query out,
	// so we need the SQL string available at commit time for the REST API request.
	if (view_info->query) {
		view_info->sql = view_info->query->ToString();
	}

	iceberg_transaction.created_views.erase(view_key);
	iceberg_transaction.created_views.emplace(view_key, std::move(view_info));

	// Invalidate stale caches
	tables.InvalidateViewCache(info.view_name);

	// Return a pointer to an owned entry (avoid dangling pointer)
	return tables.GetViewEntry(transaction.GetContext(), info.view_name);
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
	if (info.type != AlterType::ALTER_TABLE) {
		throw NotImplementedException("Only ALTER TABLE is supported for Iceberg");
	}
	auto &alter_table_info = info.Cast<AlterTableInfo>();
	auto &irc_transaction = GetICTransaction(transaction);
	auto &context = transaction.GetContext();

	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, alter_table_info.name);
	auto catalog_entry = tables.GetEntry(context, lookup);
	if (!catalog_entry) {
		throw CatalogException("Table with name \"%s\" does not exist!", alter_table_info.name);
	}
	auto &table_entry = catalog_entry->Cast<IcebergTableEntry>();
	auto &catalog_table_info = table_entry.table_info;
	irc_transaction.updated_tables.emplace(catalog_table_info.GetTableKey(), catalog_table_info.Copy());
	auto &updated_table = irc_transaction.updated_tables.at(catalog_table_info.GetTableKey());
	updated_table.InitSchemaVersions();
	updated_table.InitTransactionData(irc_transaction);

	auto &current_schema = updated_table.table_metadata.GetLatestSchema();
	// Copy the schema, then add it to the table metadata
	auto new_schema = current_schema.Copy();
	auto new_schema_id = updated_table.GetMaxSchemaId() + 1;
	new_schema->schema_id = new_schema_id;

	switch (alter_table_info.alter_table_type) {
	case AlterTableType::SET_PARTITIONED_BY: {
		auto &partition_info = alter_table_info.Cast<SetPartitionedByInfo>();

		// Ensure schema is the same as current
		updated_table.AddAssertCurrentSchemaId(irc_transaction);
		// Ensure last assigned partition field id is up to date
		updated_table.AddAssertLastAssignedPartitionId(irc_transaction);

		updated_table.SetPartitionedBy(irc_transaction, partition_info.partition_keys, *new_schema);
		return;
	}
	default: {
		throw NotImplementedException("Alter table type not supported: %s",
		                              EnumUtil::ToString(alter_table_info.alter_table_type));
	}
	}
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
	if (type == CatalogType::VIEW_ENTRY) {
		GetCatalogSet(type).ScanViews(context, callback);
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
	auto &context = transaction.GetContext();
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();

	// For VIEW_ENTRY, try the view path
	if (type == CatalogType::VIEW_ENTRY) {
		auto view_entry = GetCatalogSet(type).GetViewEntry(context, lookup_info.GetEntryName());
		if (view_entry) {
			return view_entry;
		}
		return nullptr;
	}

	// For TABLE_ENTRY, use the existing table lookup
	auto table_entry = GetCatalogSet(type).GetEntry(context, lookup_info);
	if (!table_entry) {
		// Try looking up as a view — DuckDB sometimes looks up views as TABLE_ENTRY
		auto view_entry = GetCatalogSet(type).GetViewEntry(context, lookup_info.GetEntryName());
		if (view_entry) {
			return view_entry;
		}
		// verify the schema exists
		if (!IRCAPI::VerifySchemaExistence(context, ic_catalog, name)) {
			exists = false;
			return nullptr;
		}
	}
	return table_entry;
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
