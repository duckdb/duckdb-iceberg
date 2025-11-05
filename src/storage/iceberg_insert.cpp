#include "../include/storage/iceberg_insert.hpp"
#include "storage/iceberg_insert.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_transaction.hpp"
#include "storage/irc_table_entry.hpp"
#include "storage/iceberg_table_information.hpp"
#include "metadata/iceberg_column_definition.hpp"

#include "iceberg_multi_file_list.hpp"
#include "duckdb/common/sort/partition_state.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

IcebergInsert::IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
                             physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
      column_index_map(std::move(column_index_map_p)) {
}

IcebergInsert::IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
                             unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

IcebergInsert::IcebergInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, TableCatalogEntry &table)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), table(&table), schema(nullptr) {
}

IcebergCopyInput::IcebergCopyInput(ClientContext &context, ICTableEntry &table)
    : catalog(table.catalog.Cast<IRCatalog>()), columns(table.GetColumns()) {
	data_path = table.table_info.table_metadata.GetDataPath();
}

IcebergCopyInput::IcebergCopyInput(ClientContext &context, IRCSchemaEntry &schema, const ColumnList &columns,
                                   const string &data_path_p)
    : catalog(schema.catalog.Cast<IRCatalog>()), columns(columns) {
	// When data_path_p is provided directly, it's already the table location
	// We should check if it has write.data.path property, but since this is a schema-level
	// constructor and we don't have access to table metadata, we use the default behavior
	data_path = data_path_p + "/data";
}

unique_ptr<GlobalSinkState> IcebergInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<IcebergInsertGlobalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
static string ParseQuotedValue(const string &input, idx_t &pos) {
	if (pos >= input.size() || input[pos] != '"') {
		throw InvalidInputException("Failed to parse quoted value - expected a quote");
	}
	string result;
	pos++;
	for (; pos < input.size(); pos++) {
		if (input[pos] == '"') {
			pos++;
			// check if this is an escaped quote
			if (pos < input.size() && input[pos] == '"') {
				// escaped quote
				result += '"';
				continue;
			}
			return result;
		}
		result += input[pos];
	}
	throw InvalidInputException("Failed to parse quoted value - unterminated quote");
}

static vector<string> ParseQuotedList(const string &input, char list_separator) {
	vector<string> result;
	if (input.empty()) {
		return result;
	}
	idx_t pos = 0;
	while (true) {
		result.push_back(ParseQuotedValue(input, pos));
		if (pos >= input.size()) {
			break;
		}
		if (input[pos] != list_separator) {
			throw InvalidInputException("Failed to parse list - expected a %s", string(1, list_separator));
		}
		pos++;
	}
	return result;
}

struct IcebergColumnStats {
	explicit IcebergColumnStats() = default;

	string min;
	string max;
	idx_t null_count = 0;
	idx_t column_size_bytes = 0;
	bool contains_nan = false;
	bool has_null_count = false;
	bool has_min = false;
	bool has_max = false;
	bool any_valid = true;
	bool has_contains_nan = false;
};

static IcebergColumnStats ParseColumnStats(const vector<Value> col_stats) {
	IcebergColumnStats column_stats;
	for (idx_t stats_idx = 0; stats_idx < col_stats.size(); stats_idx++) {
		auto &stats_children = StructValue::GetChildren(col_stats[stats_idx]);
		auto &stats_name = StringValue::Get(stats_children[0]);
		auto &stats_value = StringValue::Get(stats_children[1]);
		if (stats_name == "min") {
			D_ASSERT(!column_stats.has_min);
			column_stats.min = stats_value;
			column_stats.has_min = true;
		} else if (stats_name == "max") {
			D_ASSERT(!column_stats.has_max);
			column_stats.max = stats_value;
			column_stats.has_max = true;
		} else if (stats_name == "null_count") {
			D_ASSERT(!column_stats.has_null_count);
			column_stats.has_null_count = true;
			column_stats.null_count = StringUtil::ToUnsigned(stats_value);
		} else if (stats_name == "column_size_bytes") {
			column_stats.column_size_bytes = StringUtil::ToUnsigned(stats_value);
		} else if (stats_name == "has_nan") {
			column_stats.has_contains_nan = true;
			column_stats.contains_nan = stats_value == "true";
		} else {
			throw NotImplementedException("Unsupported stats type \"%s\" in IcebergInsert::Sink()", stats_name);
		}
	}
	return column_stats;
}

static void AddToColDefMap(case_insensitive_map_t<optional_ptr<IcebergColumnDefinition>> &name_to_coldef,
                           string col_name_prefix, optional_ptr<IcebergColumnDefinition> column_def) {
	string column_name = column_def->name;
	if (!col_name_prefix.empty()) {
		column_name = col_name_prefix + "." + column_def->name;
	}
	if (column_def->IsIcebergPrimitiveType()) {
		name_to_coldef.emplace(column_name, column_def.get());
	} else {
		for (auto &child : column_def->children) {
			AddToColDefMap(name_to_coldef, column_name, child.get());
		}
	}
}

static void AddWrittenFiles(IcebergInsertGlobalState &global_state, DataChunk &chunk,
                            optional_ptr<TableCatalogEntry> table) {
	D_ASSERT(table);
	// grab lock for written files vector
	lock_guard<mutex> guard(global_state.lock);
	auto &ic_table = table->Cast<ICTableEntry>();
	auto partition_id = ic_table.table_info.table_metadata.default_spec_id;
	for (idx_t r = 0; r < chunk.size(); r++) {
		IcebergManifestEntry data_file;
		data_file.file_path = chunk.GetValue(0, r).GetValue<string>();
		data_file.record_count = static_cast<int64_t>(chunk.GetValue(1, r).GetValue<idx_t>());
		data_file.file_size_in_bytes = static_cast<int64_t>(chunk.GetValue(2, r).GetValue<idx_t>());
		data_file.content = IcebergManifestEntryContentType::DATA;
		data_file.status = IcebergManifestEntryStatusType::ADDED;
		data_file.file_format = "parquet";

		if (partition_id) {
			data_file.partition_spec_id = static_cast<int32_t>(partition_id);
		} else {
			data_file.partition_spec_id = 0;
		}

		// extract the column stats
		auto column_stats = chunk.GetValue(4, r);
		auto &map_children = MapValue::GetChildren(column_stats);

		global_state.insert_count += data_file.record_count;

		auto table_current_schema_id = ic_table.table_info.table_metadata.current_schema_id;
		auto ic_schema = ic_table.table_info.table_metadata.schemas[table_current_schema_id];

		case_insensitive_map_t<optional_ptr<IcebergColumnDefinition>> column_info;
		for (auto &column : ic_schema->columns) {
			AddToColDefMap(column_info, "", column.get());
		}

		for (idx_t col_idx = 0; col_idx < map_children.size(); col_idx++) {
			auto &struct_children = StructValue::GetChildren(map_children[col_idx]);
			auto &col_name = StringValue::Get(struct_children[0]);
			auto &col_stats = MapValue::GetChildren(struct_children[1]);
			auto column_names = ParseQuotedList(col_name, '.');
			auto stats = ParseColumnStats(col_stats);
			auto normalized_col_name = StringUtil::Join(column_names, ".");

			auto ic_column_info = column_info.find(normalized_col_name);
			D_ASSERT(ic_column_info != column_info.end());
			if (ic_column_info->second->required && stats.has_null_count && stats.null_count > 0) {
				throw ConstraintException("NOT NULL constraint failed: %s.%s", table->name, normalized_col_name);
			}

			//! TODO: convert 'stats' into 'data_file.lower_bounds', upper_bounds, value_counts, null_value_counts,
			//! nan_value_counts ...
		}

		//! TODO: extract the partition info
		// auto partition_info = chunk.GetValue(5, r);
		// if (!partition_info.IsNull()) {
		//	auto &partition_children = MapValue::GetChildren(partition_info);
		//	for (idx_t col_idx = 0; col_idx < partition_children.size(); col_idx++) {
		//		auto &struct_children = StructValue::GetChildren(partition_children[col_idx]);
		//		auto &part_value = StringValue::Get(struct_children[1]);

		//		IcebergPartition file_partition_info;
		//		file_partition_info.partition_column_idx = col_idx;
		//		file_partition_info.partition_value = part_value;
		//		data_file.partition_values.push_back(std::move(file_partition_info));
		//	}
		//}

		global_state.written_files.push_back(std::move(data_file));
	}
}

SinkResultType IcebergInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergInsertGlobalState>();

	// TODO: pass through the partition id?
	AddWrittenFiles(global_state, chunk, table);

	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType IcebergInsert::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<IcebergInsertGlobalState>();
	auto value = Value::BIGINT(global_state.insert_count);
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, value);
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType IcebergInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergInsertGlobalState>();

	auto &irc_table = table->Cast<ICTableEntry>();
	auto &table_info = irc_table.table_info;
	auto &transaction = IRCTransaction::Get(context, table->catalog);

	lock_guard<mutex> guard(global_state.lock);
	if (!global_state.written_files.empty()) {
		table_info.AddSnapshot(transaction, std::move(global_state.written_files));
		transaction.MarkTableAsDirty(irc_table);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string IcebergInsert::GetName() const {
	D_ASSERT(table);
	return "ICEBERG_INSERT";
}

InsertionOrderPreservingMap<string> IcebergInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name : info->Base().table;
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
static optional_ptr<CopyFunctionCatalogEntry> TryGetCopyFunction(DatabaseInstance &db, const string &name) {
	D_ASSERT(!name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	return schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, name)->Cast<CopyFunctionCatalogEntry>();
}

static Value GetFieldIdValue(const IcebergColumnDefinition &column) {
	auto column_value = Value::BIGINT(column.id);
	if (column.children.empty()) {
		// primitive type - return the field-id directly
		return column_value;
	}
	// nested type - generate a struct and recurse into children
	child_list_t<Value> values;
	values.emplace_back("__duckdb_field_id", std::move(column_value));
	for (auto &child : column.children) {
		values.emplace_back(child->name, GetFieldIdValue(*child));
	}
	return Value::STRUCT(std::move(values));
}

static Value WrittenFieldIds(const IcebergTableSchema &schema) {
	auto &columns = schema.columns;

	child_list_t<Value> values;
	for (idx_t c_idx = 0; c_idx < columns.size(); c_idx++) {
		auto &column = columns[c_idx];
		values.emplace_back(column->name, GetFieldIdValue(*column));
	}
	return Value::STRUCT(std::move(values));
}

unique_ptr<CopyInfo> GetBindInput(IcebergCopyInput &input) {
	// Create Copy Info
	auto info = make_uniq<CopyInfo>();
	info->file_path = input.data_path;
	info->format = "parquet";
	info->is_from = false;
	for (auto &option : input.options) {
		info->options[option.first] = option.second;
	}
	return info;
}

vector<IcebergManifestEntry> IcebergInsert::GetInsertManifestEntries(IcebergInsertGlobalState &global_state) {
	lock_guard<mutex> guard(global_state.lock);
	return std::move(global_state.written_files);
}

PhysicalOperator &IcebergInsert::PlanCopyForInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   IcebergCopyInput &copy_input, optional_ptr<PhysicalOperator> plan) {
	// Get Parquet Copy function
	auto copy_fun = TryGetCopyFunction(*context.db, "parquet");
	if (!copy_fun) {
		throw MissingExtensionException("Did not find parquet copy function required to write to iceberg table");
	}

	auto names_to_write = copy_input.columns.GetColumnNames();
	auto types_to_write = copy_input.columns.GetColumnTypes();

	auto wat = GetBindInput(copy_input);
	auto bind_input = CopyFunctionBindInput(*wat);

	auto function_data = copy_fun->function.copy_to_bind(context, bind_input, names_to_write, types_to_write);

	auto &physical_copy = planner.Make<PhysicalCopyToFile>(
	    GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS), copy_fun->function,
	    std::move(function_data), 1);
	auto &physical_copy_ref = physical_copy.Cast<PhysicalCopyToFile>();

	vector<idx_t> partition_columns;
	//! TODO: support partitions
	// auto partitions = op.table.Cast<ICTableEntry>().snapshot->GetPartitionColumns();
	// if (partitions.size() != 0) {
	//	auto column_names = op.table.Cast<ICTableEntry>().GetColumns().GetColumnNames();
	//	for (int64_t i = 0; i < partitions.size(); i++) {
	//		for (int64_t j = 0; j < column_names.size(); j++) {
	//			if (column_names[j] == partitions[i]) {
	//				partition_columns.push_back(j);
	//				break;
	//			}
	//		}
	//	}
	//}

	physical_copy_ref.use_tmp_file = false;
	if (!partition_columns.empty()) {
		physical_copy_ref.filename_pattern.SetFilenamePattern("{uuidv7}");
		physical_copy_ref.file_path = copy_input.data_path;
		physical_copy_ref.partition_output = true;
		physical_copy_ref.partition_columns = partition_columns;
		physical_copy_ref.write_empty_file = true;
		physical_copy_ref.rotate = false;
	} else {
		physical_copy_ref.filename_pattern.SetFilenamePattern("{uuidv7}");
		physical_copy_ref.file_path = copy_input.data_path;
		physical_copy_ref.partition_output = false;
		physical_copy_ref.write_empty_file = false;
		physical_copy_ref.file_size_bytes = IRCatalog::DEFAULT_TARGET_FILE_SIZE;
		physical_copy_ref.rotate = true;
	}

	physical_copy_ref.file_extension = "parquet";
	physical_copy_ref.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	physical_copy_ref.per_thread_output = false;
	physical_copy_ref.return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS; // TODO: capture stats
	physical_copy_ref.write_partition_columns = true;
	if (plan) {
		physical_copy.children.push_back(*plan);
	}
	physical_copy_ref.names = names_to_write;
	physical_copy_ref.expected_types = types_to_write;
	physical_copy_ref.hive_file_pattern = true;
	return physical_copy;
}

void VerifyDirectInsertionOrder(LogicalInsert &op) {
	idx_t column_index = 0;
	for (auto &mapping : op.column_index_map) {
		if (mapping == DConstants::INVALID_INDEX || mapping != column_index) {
			//! See issue#444
			throw NotImplementedException("Iceberg inserts don't support targeted inserts yet (i.e tbl(col1,col2))");
		}
		column_index++;
	}
}

PhysicalOperator &IcebergInsert::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                            ICTableEntry &table) {
	optional_idx partition_id;
	vector<LogicalType> return_types;
	// the one return value is how many rows we are inserting
	return_types.emplace_back(LogicalType::BIGINT);
	return planner.Make<IcebergInsert>(return_types, table);
}

PhysicalOperator &IRCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                        optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into Iceberg table");
	}

	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into Iceberg table");
	}

	VerifyDirectInsertionOrder(op);

	auto &table_entry = op.table.Cast<ICTableEntry>();
	// FIXME: Inserts into V3 tables is not yet supported since
	// we need to keep track of row lineage, which we do not support
	// https://iceberg.apache.org/spec/#row-lineage
	if (table_entry.table_info.table_metadata.iceberg_version == 3) {
		throw NotImplementedException("Insert into Iceberg V3 tables");
	}
	table_entry.PrepareIcebergScanFromEntry(context);
	auto &table_info = table_entry.table_info;
	auto &schema = table_info.table_metadata.GetLatestSchema();

	auto &partition_spec = table_info.table_metadata.GetLatestPartitionSpec();
	if (!partition_spec.IsUnpartitioned()) {
		throw NotImplementedException("INSERT into a partitioned table is not supported yet");
	}
	if (table_info.table_metadata.HasSortOrder()) {
		auto &sort_spec = table_info.table_metadata.GetLatestSortOrder();
		if (sort_spec.IsSorted()) {
			throw NotImplementedException("Insert into a sorted iceberg table is not supported yet");
		}
	}

	// Create Copy Info
	auto info = make_uniq<IcebergCopyInput>(context, table_entry);

	vector<Value> field_input;
	field_input.push_back(WrittenFieldIds(schema));
	info->options["field_ids"] = std::move(field_input);

	auto &insert = planner.Make<IcebergInsert>(op, op.table, op.column_index_map);

	auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, *info, plan);

	insert.children.push_back(physical_copy);

	return insert;
}

PhysicalOperator &IRCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalCreateTable &op, PhysicalOperator &plan) {
	// TODO: parse partition information here.
	auto &schema = op.schema;

	auto &ic_schema_entry = schema.Cast<IRCSchemaEntry>();
	auto &catalog = ic_schema_entry.catalog;
	auto &irc_transaction = IRCTransaction::Get(context, catalog);

	// create the table. Takes care of committing to rest catalog and getting the metadata location etc.
	// setting the schema
	auto table = ic_schema_entry.CreateTable(irc_transaction, context, *op.info);
	if (!table) {
		throw InternalException("Table could not be created");
	}
	auto &ic_table = table->Cast<ICTableEntry>();
	// We need to load table credentials into our secrets for when we copy files
	ic_table.PrepareIcebergScanFromEntry(context);

	auto &table_schema = ic_table.table_info.table_metadata.GetLatestSchema();

	// Create Copy Info
	IcebergCopyInput copy_input(context, ic_table);
	vector<Value> field_input;
	field_input.push_back(WrittenFieldIds(table_schema));
	copy_input.options["field_ids"] = std::move(field_input);

	auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, plan);
	physical_index_vector_t<idx_t> column_index_map;
	auto &insert = planner.Make<IcebergInsert>(op, ic_table, column_index_map);

	insert.children.push_back(physical_copy);
	return insert;
}

} // namespace duckdb
