#include "storage/irc_schema_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/iceberg_table_information.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_table_entry.hpp"
#include "storage/irc_transaction.hpp"
#include "storage/table_update/common.hpp"
#include "utils/iceberg_type.hpp"
namespace duckdb {

IRCSchemaEntry::IRCSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), namespace_items(IRCAPI::ParseSchemaName(info.schema)), tables(*this) {
}

IRCSchemaEntry::~IRCSchemaEntry() {
}

IRCTransaction &GetICTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<IRCTransaction>();
}

bool IRCSchemaEntry::HandleCreateConflict(CatalogTransaction &transaction, CatalogType catalog_type,
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
		auto &irc_transaction = GetICTransaction(transaction);
		auto table_key = IcebergTableInformation::GetTableKey(namespace_items, entry_name);
		auto deleted_table_entry = irc_transaction.deleted_tables.find(table_key);
		if (deleted_table_entry != irc_transaction.deleted_tables.end()) {
			auto &ic_catalog = catalog.Cast<IRCatalog>();
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

optional_ptr<CatalogEntry> IRCSchemaEntry::CreateTable(CatalogTransaction &transaction, ClientContext &context,
                                                       BoundCreateTableInfo &info) {
	auto &base_info = info.Base();
	auto &ir_catalog = catalog.Cast<IRCatalog>();
	auto &irc_transaction = GetICTransaction(transaction);
	// check if we have an existing entry with this name
	if (!HandleCreateConflict(transaction, CatalogType::TABLE_ENTRY, base_info.table, base_info.on_conflict)) {
		return nullptr;
	}

	if (!ICTableSet::CreateNewEntry(context, ir_catalog, *this, base_info)) {
		throw InternalException("We should not be here");
	}
	auto table_key = IcebergTableInformation::GetTableKey(namespace_items, base_info.table);
	auto entry = irc_transaction.updated_tables.find(table_key);
	return entry->second.schema_versions[0].get();
}

optional_ptr<CatalogEntry> IRCSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &context = transaction.context;
	// directly create the table with stage_create = true;
	return CreateTable(transaction, *context, info);
}

void IRCSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	auto &transaction = IRCTransaction::Get(context, catalog).Cast<IRCTransaction>();
	auto table_name = info.name;
	// find if info has a table name, if so look for it in
	auto table_info_it = tables.entries.find(table_name);
	if (table_info_it == tables.entries.end()) {
		throw CatalogException("Table %s does not exist");
	}
	if (info.cascade) {
		throw NotImplementedException("DROP TABLE <table_name> CASCADE is not supported for Iceberg tables currently");
	}
	auto &table_info = table_info_it->second;
	auto table_key = table_info.GetTableKey();
	transaction.deleted_tables.emplace(table_key, table_info.Copy());
	auto &deleted_table_info = transaction.deleted_tables.at(table_key);
	// must init schema versions after copy. Schema versions have a pointer to IcebergTableInformation
	// if the IcebergTableInformation is moved, then the pointer is no longer valid.
	deleted_table_info.InitSchemaVersions();
}

void IRCSchemaEntry::ClearTableEntries() {
	tables.ClearEntries();
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
	auto &table_entry = catalog_entry->Cast<ICTableEntry>();
	auto &catalog_table_info = table_entry.table_info;
	irc_transaction.updated_tables.emplace(catalog_table_info.GetTableKey(), catalog_table_info.Copy());
	auto &updated_table = irc_transaction.updated_tables.at(catalog_table_info.GetTableKey());
	updated_table.InitSchemaVersions();
	updated_table.InitTransactionData(irc_transaction);

	// TODO: GetLatestSchema actually gets current schema. Latest != Current
	auto &current_schema = updated_table.table_metadata.GetLatestSchema();
	// Copy the schema, then add it to the table metadata
	auto new_schema = current_schema.Copy();
	auto new_schema_id = updated_table.GetMaxSchemaId() + 1;
	new_schema->schema_id = new_schema_id;

	switch (alter_table_info.alter_table_type) {
	case AlterTableType::ADD_COLUMN: {
		auto &add_info = alter_table_info.Cast<AddColumnInfo>();
		idx_t next_id = updated_table.table_metadata.GetLastColumnId();

		auto get_next_id = [&]() {
			return ++next_id;
		};

		auto iceberg_type = IcebergTypeHelper::CreateIcebergRestType(add_info.new_column.Type(), get_next_id);
		auto new_column =
		    IcebergColumnDefinition::ParseType(add_info.new_column.Name(), get_next_id(), false, iceberg_type);
		new_schema->columns.push_back(std::move(new_column));
		new_schema->last_column_id = next_id;

		auto update = make_uniq<AddSchemaUpdate>(updated_table);
		update->table_schema = new_schema;
		update->last_column_id = next_id;
		updated_table.transaction_data->updates.push_back(std::move(update));
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove_info = alter_table_info.Cast<RemoveColumnInfo>();
		bool found = false;
		for (auto it = new_schema->columns.begin(); it != new_schema->columns.end(); ++it) {
			if (StringUtil::CIEquals((*it)->name, remove_info.removed_column)) {
				new_schema->columns.erase(it);
				found = true;
				break;
			}
		}
		if (!found && !remove_info.if_column_exists) {
			throw CatalogException("Column with name \"%s\" does not exist!", remove_info.removed_column);
		}

		auto update = make_uniq<AddSchemaUpdate>(updated_table);
		update->table_schema = new_schema;
		updated_table.transaction_data->updates.push_back(std::move(update));
		break;
	}
	case AlterTableType::SET_PARTITIONED_BY: {
		auto &partition_info = alter_table_info.Cast<SetPartitionedByInfo>();
		// TODO: generate correct new spec id
		auto new_spec_id = updated_table.table_metadata.default_spec_id + 1;
		IcebergPartitionSpec new_spec;
		new_spec.spec_id = new_spec_id;

		for (auto &key : partition_info.partition_keys) {
			string column_name;
			string transform_name = "identity";

			if (key->type == ExpressionType::COLUMN_REF) {
				auto &colref = key->Cast<ColumnRefExpression>();
				column_name = colref.column_names.back();
			} else if (key->type == ExpressionType::FUNCTION) {
				auto &funcexpr = key->Cast<FunctionExpression>();
				transform_name = funcexpr.function_name;
				if (funcexpr.children.empty() || funcexpr.children[0]->type != ExpressionType::COLUMN_REF) {
					throw NotImplementedException("Only simple function transforms on columns are supported");
				}
				auto &colref = funcexpr.children[0]->Cast<ColumnRefExpression>();
				column_name = colref.column_names.back();
			} else {
				throw NotImplementedException("Unsupported partition key type: %s", key->ToString());
			}

			// Find source_id
			int32_t source_id = -1;
			for (auto &col : new_schema->columns) {
				if (StringUtil::CIEquals(col->name, column_name)) {
					source_id = col->id;
					break;
				}
			}
			if (source_id == -1) {
				throw CatalogException("Column \"%s\" not found in schema", column_name);
			}

			IcebergPartitionSpecField field;
			field.name = column_name;
			field.transform = IcebergTransform(transform_name);
			field.source_id = source_id;
			field.partition_field_id = 1000 + new_spec.fields.size(); // Use 1000+ for field IDs
			new_spec.fields.push_back(std::move(field));
		}

		updated_table.table_metadata.partition_specs[new_spec_id] = std::move(new_spec);
		updated_table.table_metadata.default_spec_id = new_spec_id;
		updated_table.AddPartitionSpec(irc_transaction);
		updated_table.SetDefaultSpec(irc_transaction);
		return;
	}
	default: {
		throw NotImplementedException("Alter table type not supported: %s",
		                              EnumUtil::ToString(alter_table_info.alter_table_type));
	}
	}
	updated_table.table_metadata.schemas.emplace(new_schema_id, std::move(new_schema));
	updated_table.table_metadata.current_schema_id = new_schema_id;
	updated_table.AddSetCurrentSchema(irc_transaction);
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
