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

#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/api/iceberg_type.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "catalog/rest/transaction/iceberg_transaction_update.hpp"

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
		if (latest_state && latest_state->status == IcebergTableStatus::DROPPED) {
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
	auto table_name = info.name;
	// find if info has a table name, if so look for it in
	auto table_info_it = tables.GetEntries().find(table_name);
	if (table_info_it == tables.GetEntries().end()) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Table %s does not exist", table_name);
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
		auto &table = transaction.DeleteTable(*table_info);
		//! FIXME: what?
		// must init schema versions after copy. Schema versions have a pointer to IcebergTableInformation
		// if the IcebergTableInformation is moved, then the pointer is no longer valid.
		table.InitSchemaVersions();
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
				    partition_field->name, partition_field->partition_field_id);
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

vector<IcebergManifestListEntry> RetrieveManifestFiles(ClientContext &context, IcebergTableInformation &updated_table) {
	auto snapshot_lookup = updated_table.GetSnapshotLookup(context);
	auto snapshot_info = updated_table.table_metadata.GetSnapshot(snapshot_lookup);

	if (!snapshot_info.snapshot) {
		return std::vector<IcebergManifestListEntry>();
	}
	IcebergOptions options;
	auto manifest_list = IcebergManifestList::Load(updated_table.BaseFilePath(), updated_table.table_metadata,
												   snapshot_info, context, options);
	return manifest_list->GetManifestListEntries();
}

//! Ensure existing data files don't contain NULL values in this column
static void VerifyNotNullConstraint(IcebergTableInformation &updated_table,
                                    IcebergColumnDefinition &column, IcebergTransaction &irc_transaction, vector<IcebergManifestListEntry> manifest_files) {
	if (manifest_files.empty()) {
		// Table is empty
		return;
	}
	bool found_column_null_count_at_least_once = false;
	for (auto &list_entry : manifest_files) {
		for (auto &manifest_entry : list_entry.manifest_entries) {
			if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}
			auto &data_file = manifest_entry.data_file;
			auto column_null_count_it = data_file.null_value_counts.find(column.id);
			auto found_column_null_count = column_null_count_it != data_file.null_value_counts.end();
			found_column_null_count_at_least_once = found_column_null_count_at_least_once || found_column_null_count;
			// `null_value_counts` is an optional field per the Iceberg spec.
			if (found_column_null_count && column_null_count_it->second > 0) {
				throw ConstraintException("NOT NULL constraint failed: %s.%s", updated_table.name, column.name);
			}
		}
	}

	if (!found_column_null_count_at_least_once && (!column.initial_default || column.initial_default->IsNull())) {
		/* The column is present in current_schema but not in manifest/snapshots, without a default value, and
		 * table is not empty.
		 *
		 * So now we know that either:
		 * for this column all rows are null
		 * OR
		 * for this column the optional field `null_value_counts` is not present in any manifest.
		 *
		 * In either case the constraint fails. We could possibly read the avro file's key-value metadata to be able to
		 * discern between these two cases, and in the case where the optional field is missing show a different error
		 * message, e.g. "null_value_counts required for `ALTER ... SET NOT NULL`" , or eventually do a scan to check
		 * for nulls (time intensive).
		 *
		 * We would discern between the two cases as follows:
		 *	if (no manifest is using the current_schema_id) then we know:
		 *	for this column all rows are null
		 *	ELSE
		 *	for this column the optional field `null_value_counts`  is not present in any manifest.
		 */
		throw ConstraintException("NOT NULL constraint failed: %s.%s", updated_table.name, column.name);
	}
}

void IntroduceNewSchema(IcebergTableInformation &updated_table, IcebergTransactionData &transaction_data,
                        shared_ptr<IcebergTableSchema> new_schema) {
	auto new_schema_id = new_schema->schema_id;
	updated_table.CreateSchemaVersion(*new_schema);
	transaction_data.TableAddSchema(new_schema_id);

	// Update the Table Metadata to have our new schema
	updated_table.table_metadata.AddSchema(std::move(new_schema));
	updated_table.table_metadata.SetCurrentSchemaId(new_schema_id);
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

		// Add the new column
		auto new_iceberg_column = make_uniq<IcebergColumnDefinition>();
		auto &last_column_id = updated_table.table_metadata.last_column_id;
		if (!last_column_id.IsValid()) {
			throw InternalException("No last_column_id when trying to ADD COLUMN %s", add_column_info.name);
		}
		new_iceberg_column->id = last_column_id.GetIndex() + 1;
		last_column_id = optional_idx(new_iceberg_column->id);

		new_iceberg_column->name = column_definition.GetName();
		new_iceberg_column->type = column_definition.GetType();

		if (column_definition.HasDefaultValue()) {
			auto &default_value = column_definition.DefaultValue();

			/*TODO: Support more expressions.
			 *  Which expressions should we support? Some will require binding, should that binding happen here?
			 *  ExtractInitialValue in iceberg_create_table_request.cpp:208-216 gets a value using a ConstantBinder.
			 */
			switch (default_value.type) {
			case ExpressionType::VALUE_CONSTANT: {
				auto &default_constant_value = default_value.Cast<ConstantExpression>().value;
				if (default_constant_value.IsNull()) {
					break;
				}
				if (new_iceberg_column->type != default_constant_value.type()) {
					throw InvalidInputException(
					    "Type mismatch between new COLUMN %s type: %s and DEFAULT value type: %s",
					    new_iceberg_column->name, new_iceberg_column->type.ToString(),
					    default_constant_value.type().ToString());
				}
				new_iceberg_column->initial_default = make_uniq<Value>(default_constant_value);
				break;
			}
			case ExpressionType::VALUE_NULL:
				break;
			default:
				throw InvalidInputException("DEFAULT expression not yet supported");
			}
		}

		new_iceberg_column->required = false;

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
			    to_remove_column, partition_field->name, partition_field->partition_field_id);
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
		auto &set_not_null_info = alter_table_info.Cast<SetNotNullInfo>();

		const auto new_schema = current_schema.Copy();
		new_schema->schema_id++;

		auto &column = ResolveColumn<SetNotNullInfo>(set_not_null_info, new_schema);

		// Use IcebergTransactionData existing_manifest_list if available
		const vector<IcebergManifestListEntry> manifest_files = !transaction_data.existing_manifest_list.empty()
		                                                      ? transaction_data.existing_manifest_list
		                                                      : RetrieveManifestFiles(context, updated_table);

		VerifyNotNullConstraint(updated_table, column, irc_transaction, manifest_files);

		column.required = true;

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
	auto table_entry = GetCatalogSet(type).GetEntry(context, lookup_info);
	if (!table_entry) {
		// verify the schema exists
		if (!IRCAPI::VerifySchemaExistence(context, ic_catalog, name)) {
			// set exists to false here
			// we would like to throw an error, but this code is also called when listing schemas,
			// and throwing an error will abort the listing process.
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
