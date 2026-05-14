#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"

#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/api/iceberg_type.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"
#include "common/iceberg_default.hpp"
#include "duckdb/common/exception/http_exception.hpp"

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
		auto latest_state = iceberg_transaction.GetLatestTableState(table_key);
		if (latest_state && latest_state->IsDroppedOrRenamed()) {
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
		throw NotImplementedException("DuckDB-Iceberg, Unsupported conflict type: %s", EnumUtil::ToString(on_conflict));
	}
	return true;
}

optional_ptr<CatalogEntry> IcebergSchemaEntry::CreateTable(CatalogTransaction &transaction, ClientContext &context,
                                                           BoundCreateTableInfo &info) {
	auto &iceberg_transaction = IcebergTransaction::Get(context, catalog);
	if (!exists && iceberg_transaction.created_schemas.find(name) == iceberg_transaction.created_schemas.end()) {
		throw InvalidInputException("Schema with name \"%s\" does not exist", name);
	}
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

	// CASCADE is not part of the Iceberg REST spec — reject before any type-specific handling.
	if (info.cascade) {
		if (info.type == CatalogType::VIEW_ENTRY) {
			throw NotImplementedException("DROP VIEW <view_name> CASCADE is not supported for Iceberg views currently");
		}
		throw NotImplementedException("DROP TABLE <table_name> CASCADE is not supported for Iceberg tables currently");
	}

	switch (info.type) {
	case CatalogType::VIEW_ENTRY: {
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
	case CatalogType::TABLE_ENTRY: {
		auto table_info_it = tables.GetEntries().find(entry_name);
		if (table_info_it == tables.GetEntries().end()) {
			if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
				return;
			}
			throw CatalogException("Table %s does not exist", entry_name);
		}
		if (delete_entry) {
			// Remove the entry from the catalog
			tables.GetEntriesMutable().erase(entry_name);
		} else {
			// Add the table to the transaction's deleted_tables
			auto &transaction = IcebergTransaction::Get(context, catalog).Cast<IcebergTransaction>();
			auto &table_info = table_info_it->second;
			auto &table = transaction.DeleteTable(*table_info);
			//! FIXME: what?
			// must init schema versions after copy. Schema versions have a pointer to IcebergTableInformation
			// if the IcebergTableInformation is moved, then the pointer is no longer valid.
			table.InitSchemaVersions();
		}
		return;
	}
	default:
		throw NotImplementedException("DropEntry not implemented for CatalogType '%s'", CatalogTypeToString(info.type));
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

	auto view_info = unique_ptr_cast<CreateInfo, CreateViewInfo>(info.Copy());
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

static void VerifySchemaEvolution(const IcebergTableMetadata &table_metadata, const IcebergColumnDefinition &column,
                                  const LogicalType &target_type) {
	auto &original_type = column.type;

	string extra_info;
	switch (original_type.id()) {
	case LogicalTypeId::DECIMAL: {
		if (target_type.id() != LogicalTypeId::DECIMAL) {
			break;
		}
		uint8_t width;
		uint8_t scale;
		original_type.GetDecimalProperties(width, scale);

		uint8_t other_width;
		uint8_t other_scale;
		target_type.GetDecimalProperties(other_width, other_scale);

		if (scale != other_scale) {
			extra_info = "(DECIMAL evolution has to preserve the original scale, for reference: DECIMAL(width, scale))";
			break;
		}
		if (other_width < width) {
			extra_info =
			    "(DECIMAL evolution can only increase the width, not lower it, for reference: DECIMAL(width, scale))";
			break;
		}
		return;
	}
	case LogicalTypeId::INTEGER: {
		if (target_type.id() != LogicalTypeId::BIGINT) {
			break;
		}
		return;
	}
	case LogicalTypeId::FLOAT: {
		if (target_type.id() != LogicalTypeId::DOUBLE) {
			break;
		}
		return;
	}
	case LogicalTypeId::DATE: {
		if (target_type.id() == LogicalTypeId::TIMESTAMP || target_type.id() == LogicalTypeId::TIMESTAMP_NS) {
			auto &partition_spec = table_metadata.GetLatestPartitionSpec();
			auto partition_field = partition_spec.TryGetFieldBySourceId(column.id);
			if (partition_field) {
				extra_info = StringUtil::Format(
				    " (there is a partition field that refers to the column (name: %s, partition_field_id: %d))",
				    partition_field->GetPartitionSpecFieldName(), partition_field->partition_field_id);
				break;
			}
			if (target_type.id() == LogicalTypeId::TIMESTAMP_NS) {
				if (table_metadata.iceberg_version >= 3) {
					return;
				}
				extra_info = " (DATE to TIMESTAMP_NS is a Iceberg V3 feature)";
				break;
			}
			return;
		}
		break;
	}
	default:
		break;
	}
	auto error = StringUtil::Format("Column '%s' of type '%s' can't be altered to type '%s'%s", column.name,
	                                original_type.ToString(), target_type.ToString(), extra_info);
	throw CatalogException(error);
}

void IntroduceNewSchema(IcebergTableInformation &updated_table, IcebergTransactionData &transaction_data,
                        shared_ptr<IcebergTableSchema> new_schema) {
	auto new_schema_id = new_schema->schema_id;

	auto &result_schema = updated_table.table_metadata.AddSchemaOrGetExisting(std::move(new_schema));
	if (result_schema.schema_id == new_schema_id) {
		// Update the Table Metadata to have our new schema
		updated_table.CreateSchemaVersion(result_schema);
		transaction_data.TableAddSchema(new_schema_id);
	} else {
		transaction_data.TableSetCurrentSchema();
	}
	updated_table.table_metadata.SetCurrentSchemaId(result_schema.schema_id);
}

template <typename T>
IcebergColumnDefinition &ResolveColumn(T &alter_table_info, const shared_ptr<IcebergTableSchema> &new_schema) {
	auto &column_name = alter_table_info.column_name;

	auto column_p = new_schema->GetMutableFromPath({column_name}, nullptr);
	if (!column_p) {
		throw CatalogException("Column with name '%s' does not exist on the table '%s'", column_name,
		                       alter_table_info.GetAlterEntryData().name);
	}
	auto &column = *column_p;
	return column;
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

	auto &alter = irc_transaction.GetOrCreateAlter();
	auto &updated_table = alter.GetOrInitializeTable(catalog_table_info);
	auto &transaction_data = updated_table.GetOrCreateTransactionData(irc_transaction);
	auto &current_schema = updated_table.table_metadata.GetLatestSchema();

	switch (alter_table_info.alter_table_type) {
	case AlterTableType::SET_PARTITIONED_BY: {
		auto &partition_info = alter_table_info.Cast<SetPartitionedByInfo>();

		// Ensure schema is the same as current
		transaction_data.TableAddAssertCurrentSchemaId();
		// Ensure last assigned partition field id is up to date
		transaction_data.TableAddAssertLastAssignedPartitionId();

		updated_table.SetPartitionedBy(irc_transaction, partition_info.partition_keys, current_schema);
		return;
	}
	case AlterTableType::ADD_COLUMN: {
		auto &add_column_info = alter_table_info.Cast<AddColumnInfo>();
		auto &column_definition = add_column_info.new_column;
		if (column_definition.GetType().IsNested()) {
			throw NotImplementedException("ADD COLUMN for Nested Types not supported for Iceberg tables");
		}

		if (add_column_info.if_column_not_exists) {
			for (auto &col : current_schema.columns) {
				if (col->name == column_definition.GetName()) {
					return;
				}
			}
		}

		auto &last_column_id = updated_table.table_metadata.last_column_id;
		if (!last_column_id.IsValid()) {
			throw InternalException("No last_column_id when trying to ADD COLUMN %s", add_column_info.name);
		}
		auto field_id = last_column_id.GetIndex() + 1;
		auto next_field_id = [&field_id]() -> idx_t {
			return field_id++;
		};

		IcebergDefaultBinder binder(context);
		auto new_iceberg_column = IcebergCreateTableRequest::CreateIcebergColumn(
		    column_definition, binder, false, next_field_id, updated_table.table_metadata.iceberg_version);
		last_column_id = field_id - 1;

		auto new_schema = current_schema.Copy();
		new_schema->schema_id++;
		new_schema->columns.push_back(std::move(new_iceberg_column));

		IntroduceNewSchema(updated_table, transaction_data, new_schema);

		return;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove_column_info = alter_table_info.Cast<RemoveColumnInfo>();
		auto &to_remove_column = remove_column_info.removed_column;

		if (remove_column_info.cascade) {
			throw NotImplementedException("CASCADE is not implemented for Iceberg table DROP COLUMN");
		}

		optional_idx column_id;
		auto new_schema = current_schema.RemoveColumn(to_remove_column, column_id);
		const bool column_exists = column_id.IsValid();
		if (!column_exists) {
			if (!remove_column_info.if_column_exists) {
				throw CatalogException(
				    "Attempted to drop column '%s' from table '%s', but no column by this name exists "
				    "in the current schema (id: %d)",
				    to_remove_column, table_entry.name, current_schema.schema_id);
			}
			//! Column doesn't exist, just return
			return;
		}

		auto &partition_spec = updated_table.table_metadata.GetLatestPartitionSpec();
		auto partition_field = partition_spec.TryGetFieldBySourceId(column_id.GetIndex());
		if (partition_field) {
			throw CatalogException(
			    "Can't drop column '%s' as it is referenced by the current partition spec's field: '%s' (field id: %d)",
			    to_remove_column, partition_field->GetPartitionSpecFieldName(), partition_field->partition_field_id);
		}

		if (new_schema->columns.empty()) {
			throw CatalogException("Cannot drop column: table '%s' only has one column remaining!", table_entry.name);
		}

		IntroduceNewSchema(updated_table, transaction_data, new_schema);

		return;
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change_type_info = alter_table_info.Cast<ChangeColumnTypeInfo>();

		auto new_schema = current_schema.Copy();
		new_schema->schema_id++;

		auto &column = ResolveColumn<ChangeColumnTypeInfo>(change_type_info, new_schema);

		if (change_type_info.expression->type != ExpressionType::OPERATOR_CAST) {
			throw NotImplementedException("ALTER TYPE with a USING expression is not supported for Iceberg tables");
		}
		VerifySchemaEvolution(updated_table.table_metadata, column, change_type_info.target_type);
		column.type = change_type_info.target_type;

		IntroduceNewSchema(updated_table, transaction_data, new_schema);
		return;
	}
	case AlterTableType::SET_NOT_NULL: {
		// Column integrity is not transactionally guaranteed by Iceberg catalogs during SET NOT NULL
		throw InvalidInputException("Cannot change nullable column to non-nullable");
	}
	case AlterTableType::DROP_NOT_NULL: {
		auto &drop_not_null_info = alter_table_info.Cast<DropNotNullInfo>();

		auto new_schema = current_schema.Copy();
		new_schema->schema_id++;

		auto &column = ResolveColumn<DropNotNullInfo>(drop_not_null_info, new_schema);

		column.required = false;

		IntroduceNewSchema(updated_table, transaction_data, new_schema);
		return;
	}
	case AlterTableType::RENAME_TABLE: {
		auto &rename_table_info = alter_table_info.Cast<RenameTableInfo>();
		auto &new_name = rename_table_info.new_table_name;

		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, new_name);
		auto other_catalog_entry = tables.GetEntry(context, lookup);
		if (other_catalog_entry) {
			//! The table exists at this point, check if it was deleted/renamed in the transaction
			auto &other_table_entry = other_catalog_entry->Cast<IcebergTableEntry>();
			auto &other_table_info = other_table_entry.table_info;
			auto other_table_key = other_table_info.GetTableKey();
			auto state = irc_transaction.GetLatestTableState(other_table_key);
			if (!state || state->IsAlive()) {
				throw CatalogException("Table with name \"%s\" already exists!", new_name);
			}
			//! The table is dropped or renamed by this transaction, so it's not a conflict anymore
			D_ASSERT(state && state->IsDroppedOrRenamed());
		}
		irc_transaction.RenameTable(updated_table, new_name);
		break;
	}
	case AlterTableType::RENAME_COLUMN: {
		auto &rename_info = alter_table_info.Cast<RenameColumnInfo>();
		auto &column_name = rename_info.old_name;
		auto &new_name = rename_info.new_name;

		auto new_schema = current_schema.Copy();
		new_schema->schema_id++;

		auto column_p = new_schema->GetMutableFromPath({column_name}, nullptr);
		if (!column_p) {
			throw CatalogException("Column with name '%s' does not exist on the table '%s', RENAME COLUMN failed",
			                       column_name, table_entry.name);
		}
		auto collision_column_p = new_schema->GetMutableFromPath({new_name}, nullptr);
		if (collision_column_p) {
			throw CatalogException("Column with name '%s' already exists on the table '%s', RENAME COLUMN failed",
			                       new_name, table_entry.name);
		}
		auto &column = *column_p;
		column.name = new_name;

		auto new_schema_id = new_schema->schema_id;

		auto &result_schema = updated_table.table_metadata.AddSchemaOrGetExisting(std::move(new_schema));
		if (result_schema.schema_id == new_schema_id) {
			// Update the Table Metadata to have our new schema
			updated_table.CreateSchemaVersion(result_schema);
			transaction_data.TableAddSchema(new_schema_id);
		} else {
			transaction_data.TableSetCurrentSchema();
		}
		updated_table.table_metadata.SetCurrentSchemaId(result_schema.schema_id);
		return;
	}
	case AlterTableType::SET_TABLE_OPTIONS: {
		auto &set_options_info = alter_table_info.Cast<SetTableOptionsInfo>();

		auto binder_ptr = Binder::CreateBinder(context);
		TableFunctionBinder property_binder(*binder_ptr, context, "SET TABLE OPTIONS");

		optional_idx new_format_version;
		case_insensitive_map_t<string> new_properties;

		for (auto &option : set_options_info.table_options) {
			auto &key = option.first;
			auto expr_copy = option.second->Copy();
			auto bound_expr = property_binder.Bind(expr_copy);
			if (bound_expr->HasParameter()) {
				throw ParameterNotResolvedException();
			}
			auto val = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
			if (val.IsNull()) {
				throw BinderException("NULL is not supported as a valid option for '%s'", key);
			}

			if (StringUtil::CIEquals(key, "format-version")) {
				if (!val.DefaultTryCastAs(LogicalType::INTEGER, true)) {
					throw InvalidInputException("Can't cast 'format-version' property (%s) to INTEGER", val.ToString());
				}
				new_format_version = val.GetValue<int32_t>();
			} else {
				if (!val.DefaultTryCastAs(LogicalType::VARCHAR, true)) {
					throw InvalidInputException("Can't cast '%s' property (%s) to VARCHAR", key, val.ToString());
				}
				new_properties[key] = val.GetValue<string>();
			}
		}

		if (new_format_version.IsValid()) {
			auto current_version = updated_table.table_metadata.iceberg_version;
			if ((int32_t)new_format_version.GetIndex() < current_version) {
				throw InvalidInputException("Cannot downgrade format-version from %d to %d", current_version,
				                            new_format_version.GetIndex());
			}
			updated_table.table_metadata.iceberg_version = (int32_t)new_format_version.GetIndex();
			transaction_data.TableAddUpradeFormatVersion();
		}

		if (!new_properties.empty()) {
			transaction_data.TableSetProperties(new_properties);
			for (auto &prop : new_properties) {
				updated_table.table_metadata.table_properties[prop.first] = prop.second;
			}
		}

		return;
	}
	case AlterTableType::RESET_TABLE_OPTIONS: {
		auto &reset_options_info = alter_table_info.Cast<ResetTableOptionsInfo>();

		vector<string> properties_to_remove(reset_options_info.table_options.begin(),
		                                    reset_options_info.table_options.end());
		if (!properties_to_remove.empty()) {
			transaction_data.TableRemoveProperties(properties_to_remove);
			for (auto &key : properties_to_remove) {
				updated_table.table_metadata.table_properties.erase(key);
			}
		}
		return;
	}
	case AlterTableType::SET_DEFAULT: {
		auto &set_default_info = alter_table_info.Cast<SetDefaultInfo>();
		auto &column_name = set_default_info.column_name;
		auto &expression = set_default_info.expression;

		auto new_schema = current_schema.Copy();
		new_schema->schema_id++;

		auto column_p = new_schema->GetMutableFromPath({column_name}, nullptr);
		if (!column_p) {
			throw CatalogException("Column with name '%s' does not exist on the table '%s', SET DEFAULT failed",
			                       column_name, table_entry.name);
		}
		auto &column = *column_p;
		if (updated_table.table_metadata.iceberg_version < 3) {
			throw NotImplementedException("SET DEFAULT is not supported on tables < V3");
		}

		IcebergDefaultBinder binder(context);
		auto default_constant_value = binder.Evaluate(expression.get(), column.type);
		column.write_default = make_uniq<Value>(default_constant_value);

		auto new_schema_id = new_schema->schema_id;

		auto &result_schema = updated_table.table_metadata.AddSchemaOrGetExisting(std::move(new_schema));
		if (result_schema.schema_id == new_schema_id) {
			// Update the Table Metadata to have our new schema
			updated_table.CreateSchemaVersion(result_schema);
			transaction_data.TableAddSchema(new_schema_id);
		} else {
			transaction_data.TableSetCurrentSchema();
		}
		updated_table.table_metadata.SetCurrentSchemaId(result_schema.schema_id);
		return;
	}
	case AlterTableType::ADD_FIELD: {
		auto &add_field_info = alter_table_info.Cast<AddFieldInfo>();
		auto &column_path = add_field_info.column_path;
		auto &new_field = add_field_info.new_field;
		auto &if_field_not_exists = add_field_info.if_field_not_exists;

		auto new_schema = current_schema.Copy();
		new_schema->schema_id++;

		auto parent_path = column_path;
		column_path.push_back(new_field.GetName());

		auto parent_p = new_schema->GetMutableFromPath(parent_path, nullptr);
		if (!parent_p) {
			throw CatalogException(
			    "The parent column ('%s') does not exist on the table '%s', ADD COLUMN failed to add a new field",
			    StringUtil::Join(parent_path, "."), table_entry.name);
		}
		auto &parent = *parent_p;

		auto column_p = new_schema->GetMutableFromPath(column_path, nullptr);
		if (column_p) {
			if (if_field_not_exists) {
				return;
			}
			throw CatalogException(
			    "The column ('%s') already exists on the table '%s', ADD COLUMN failed to add a new field",
			    StringUtil::Join(column_path, "."), table_entry.name);
		}

		if (parent.type.id() != LogicalTypeId::STRUCT) {
			throw CatalogException("Can't add field '%s' to column '%s', because the parent is not a struct (type: %s)",
			                       new_field.GetName(), StringUtil::Join(parent_path, "."), parent.type.ToString());
		}

		auto &last_column_id = updated_table.table_metadata.last_column_id;
		if (!last_column_id.IsValid()) {
			throw InternalException("No last_column_id when trying to ADD COLUMN %s",
			                        StringUtil::Join(column_path, "."));
		}
		auto field_id = last_column_id.GetIndex() + 1;
		auto next_field_id = [&field_id]() -> idx_t {
			return field_id++;
		};

		IcebergDefaultBinder binder(context);
		auto new_iceberg_column = IcebergCreateTableRequest::CreateIcebergColumn(
		    new_field, binder, false, next_field_id, updated_table.table_metadata.iceberg_version);
		last_column_id = field_id - 1;

		parent.children.push_back(std::move(new_iceberg_column));

		IntroduceNewSchema(updated_table, transaction_data, new_schema);
		return;
	}
	case AlterTableType::RENAME_FIELD: {
		auto &rename_field_info = alter_table_info.Cast<RenameFieldInfo>();
		auto &column_path = rename_field_info.column_path;
		auto &new_name = rename_field_info.new_name;

		auto new_schema = current_schema.Copy();
		new_schema->schema_id++;

		auto column_p = new_schema->GetMutableFromPath(column_path, nullptr);
		if (!column_p) {
			throw CatalogException("The column ('%s') doesn't exist on the table '%s', RENAME COLUMN failed",
			                       StringUtil::Join(column_path, "."), table_entry.name);
		}

		auto new_path = column_path;
		new_path.pop_back();
		new_path.push_back(new_name);

		auto existing_column = new_schema->GetMutableFromPath(new_path, nullptr);
		if (existing_column) {
			throw CatalogException(
			    "The column ('%s') already exists on the table '%s', RENAME COLUMN failed to rename the field",
			    StringUtil::Join(new_path, "."), table_entry.name);
		}
		column_p->name = new_name;
		IntroduceNewSchema(updated_table, transaction_data, new_schema);
		return;
	}
	case AlterTableType::REMOVE_FIELD: {
		auto &remove_field_info = alter_table_info.Cast<RemoveFieldInfo>();
		auto &column_path = remove_field_info.column_path;
		auto &cascade = remove_field_info.cascade;
		auto &if_column_exists = remove_field_info.if_column_exists;

		auto new_schema = current_schema.Copy();
		new_schema->schema_id++;

		D_ASSERT(column_path.size() > 1);
		auto parent_path = column_path;
		parent_path.pop_back();

		if (cascade) {
			throw NotImplementedException("DROP COLUMN with CASCADE is not implemented for Iceberg tables");
		}

		auto parent_p = new_schema->GetMutableFromPath(parent_path, nullptr);
		if (!parent_p) {
			if (if_column_exists) {
				return;
			}
			throw CatalogException(
			    "The column ('%s') doesnt exist on the table '%s', DROP COLUMN failed to remove the field",
			    StringUtil::Join(column_path, "."), table_entry.name);
		}
		auto &parent = *parent_p;
		auto child_it = parent.GetChildIterator(column_path.back());
		if (child_it == parent.children.end()) {
			if (if_column_exists) {
				return;
			}
			throw CatalogException(
			    "The column ('%s') doesnt exist on the table '%s', DROP COLUMN failed to remove the field",
			    StringUtil::Join(column_path, "."), table_entry.name);
		}
		if (parent.children.size() == 1) {
			throw CatalogException("Can't drop field '%s' because it's the last field of the STRUCT!",
			                       StringUtil::Join(column_path, "."));
		}
		parent.children.erase(child_it);
		IntroduceNewSchema(updated_table, transaction_data, new_schema);
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

void IcebergSchemaEntry::LoadProperties(ClientContext &context) {
	if (schema_info.properties_loaded) {
		// not needed
		return;
	}
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();

	auto get_namespace_result = IRCAPI::GetNamespace(context, ic_catalog, *this);
	if (get_namespace_result.has_error) {
		throw HTTPException(StringUtil::Format("GetNamespace endpoint returned response code %s with message \"%s\"",
		                                       EnumUtil::ToString(get_namespace_result.status_),
		                                       get_namespace_result.error_._error.message));
	}

	schema_info.properties = get_namespace_result.result_->properties;
	schema_info.properties_loaded = true;
	// TODO: eventually set up caching for this response?
};

} // namespace duckdb
