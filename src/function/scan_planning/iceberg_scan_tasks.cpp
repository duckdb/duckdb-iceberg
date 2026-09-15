#include "function/iceberg_functions.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "planning/scan_plan/iceberg_scan_task.hpp"

namespace duckdb {

using TaskFormat = IcebergScanTaskFormat;

struct IcebergScanTasksBindData : public TableFunctionData {
	vector<idx_t> columns;
	vector<int32_t> partition_ids;
	LogicalType schema_type;
};

struct IcebergTaskExecutionContext {
	IcebergTableMetadata metadata {IcebergTableMetadataSchemas()};
	optional_ptr<const IcebergTableSchema> schema;
	IcebergOptions options;
	IcebergDeleteExecutionState deletes;
};

struct IcebergScanTasksGlobalState : public GlobalTableFunctionState {
	mutex lock;
	string metadata_json;
	Value schema_id;
	Value snapshot_id;
	shared_ptr<IcebergTaskExecutionContext> execution;

	idx_t MaxThreads() const override {
		return MAX_THREADS;
	}

	shared_ptr<IcebergTaskExecutionContext> GetContext(ClientContext &context, const IcebergScanTasksBindData &bind,
	                                                   const Value &json, const Value &schema, const Value &snapshot) {
		if (json.IsNull() || schema.IsNull()) {
			throw InvalidInputException("iceberg_scan_tasks metadata and schema_id cannot be NULL");
		}
		lock_guard<mutex> guard(lock);
		auto &text = StringValue::Get(json);
		if (execution) {
			if (text != metadata_json || !Value::NotDistinctFrom(schema, schema_id) ||
			    !Value::NotDistinctFrom(snapshot, snapshot_id)) {
				throw InvalidInputException(
				    "iceberg_scan_tasks requires one metadata document, schema ID, and snapshot ID");
			}
			return execution;
		}
		auto result = make_shared_ptr<IcebergTaskExecutionContext>();
		auto doc = JSONDocument::Parse(text.c_str(), text.size());
		auto metadata = rest_api_objects::TableMetadata::FromJSON(doc->GetRoot());
		if (!metadata.location || !metadata.last_updated_ms) {
			throw InvalidInputException("iceberg_scan_tasks metadata requires location and last-updated-ms");
		}
		if (metadata.schemas) {
			for (auto &item : *metadata.schemas) {
				if (!item.object_1.schema_id) {
					throw InvalidInputException("iceberg_scan_tasks metadata schema requires schema-id");
				}
			}
		}
		if (metadata.partition_specs) {
			for (auto &item : *metadata.partition_specs) {
				if (!item.spec_id) {
					throw InvalidInputException("iceberg_scan_tasks metadata partition spec requires spec-id");
				}
			}
		}
		result->metadata = IcebergTableMetadata::FromTableMetadata(metadata);
		result->metadata.GetSchemas().ForEachSchema([&](const IcebergTableSchema &candidate) {
			if (candidate.schema_id == IntegerValue::Get(schema)) {
				result->schema = candidate;
			}
		});
		if (!result->schema) {
			throw InvalidInputException("iceberg_scan_tasks schema_id is absent from the metadata");
		}
		if (TaskFormat::SchemaType(*result->schema) != bind.schema_type) {
			throw InvalidInputException("iceberg_scan_tasks schema column does not match the selected metadata schema");
		}
		if (!snapshot.IsNull() && !result->metadata.GetSnapshotById(BigIntValue::Get(snapshot))) {
			throw InvalidInputException("iceberg_scan_tasks snapshot_id is absent from the metadata");
		}
		metadata_json = text;
		schema_id = schema;
		snapshot_id = snapshot;
		execution = result;
		return result;
	}
};

struct IcebergTaskScanInfo : public TableFunctionInfo {
	shared_ptr<IcebergTaskExecutionContext> execution;
	OpenFileInfo file;
	unordered_map<int32_t, Value> partition_constants;
	vector<Value> delete_files;
};

//! The ordinary reader owns mapping and chunk finalization. Only its planner-facing
//! binding and task lookup are replaced for a materialized single-file task.
struct IcebergTaskReader : public IcebergMultiFileReader {
	explicit IcebergTaskReader(shared_ptr<TableFunctionInfo> info) : IcebergMultiFileReader(std::move(info)) {
	}

	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &function) {
		return make_uniq<IcebergTaskReader>(function.function_info);
	}

	shared_ptr<MultiFileList> CreateFileList(ClientContext &, const vector<string> &, const FileGlobInput &) override {
		auto &info = function_info->Cast<IcebergTaskScanInfo>();
		return make_shared_ptr<SimpleMultiFileList>(vector<OpenFileInfo> {info.file});
	}

	bool Bind(MultiFileOptions &, MultiFileList &, vector<LogicalType> &types, vector<Identifier> &names,
	          MultiFileReaderBindData &bind) override {
		auto &info = function_info->Cast<IcebergTaskScanInfo>();
		for (auto &column : info.execution->schema->columns) {
			types.push_back(column->type);
			names.emplace_back(column->name);
			bind.schema.push_back(column->GetMultiFileColumnDefinition());
		}
		QueryResult::DeduplicateColumns(names);
		for (idx_t i = 0; i < names.size(); i++) {
			bind.schema[i].name = names[i];
		}
		bind.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;
		return true;
	}

	ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind,
	                                      const vector<MultiFileColumnDefinition> &columns,
	                                      const vector<ColumnIndex> &column_ids, optional_ptr<TableFilterSet> filters,
	                                      ClientContext &context, MultiFileGlobalState &gstate) override {
		auto &info = function_info->Cast<IcebergTaskScanInfo>();
		auto &execution = *info.execution;
		IcebergDeleteExecutionContext delete_context {context, FileSystem::GetFileSystem(context),
		                                              execution.metadata.location, execution.options,
		                                              execution.metadata};
		auto deletes = execution.deletes.ProcessDeletes(delete_context, info.file.path, info.delete_files);
		return InitializeTaskReader(reader_data, bind, columns, column_ids, filters, context, gstate,
		                            execution.metadata.GetSchemas(), execution.metadata.mappings, std::move(deletes),
		                            info.partition_constants);
	}
};

//! Member order keeps bind data, descriptors and execution context alive until
//! both scanner states have been destroyed.
struct IcebergActiveTask {
	TableFunction function;
	unique_ptr<FunctionData> bind;
	unique_ptr<GlobalTableFunctionState> global;
	unique_ptr<LocalTableFunctionState> local;
};

struct IcebergScanTasksLocalState : public LocalTableFunctionState {
	idx_t row = 0;
	Vector metadata_json {LogicalType::JSON()};
	unique_ptr<IcebergActiveTask> active;
};

static unique_ptr<FunctionData> IcebergScanTasksBind(ClientContext &, TableFunctionBindInput &input,
                                                     vector<LogicalType> &types, vector<Identifier> &names) {
	auto result = make_uniq<IcebergScanTasksBindData>();
	case_insensitive_map_t<idx_t> indexes;
	for (idx_t i = 0; i < input.input_table_names.size(); i++) {
		if (!indexes.emplace(input.input_table_names[i].GetIdentifierName(), i).second) {
			throw BinderException("iceberg_scan_tasks input contains duplicate column '%s'",
			                      input.input_table_names[i]);
		}
	}
	auto expected = TaskFormat::Columns(LogicalType::STRUCT({}), LogicalType::STRUCT({}));
	for (idx_t i = 0; i < expected.size(); i++) {
		auto entry = indexes.find(expected[i].first.GetIdentifierName());
		if (entry == indexes.end()) {
			throw BinderException("iceberg_scan_tasks missing required input column '%s'",
			                      expected[i].first.GetIdentifierName());
		}
		auto &type = input.input_table_types[entry->second];
		bool valid = i == TaskFormat::PARTITION_CONSTANTS || i == TaskFormat::SCHEMA
		                 ? type.id() == LogicalTypeId::STRUCT
		                 : type == expected[i].second;
		if (!valid) {
			throw BinderException("iceberg_scan_tasks input column '%s' has unexpected type %s (expected %s)",
			                      expected[i].first.GetIdentifierName(), type.ToString(),
			                      expected[i].second.ToString());
		}
		result->columns.push_back(entry->second);
	}
	result->schema_type = input.input_table_types[result->columns[TaskFormat::SCHEMA]];
	for (auto &column : StructType::GetChildTypes(result->schema_type)) {
		names.emplace_back(column.first);
		types.push_back(column.second);
	}
	if (types.empty()) {
		throw BinderException("iceberg_scan_tasks schema must contain at least one column");
	}
	auto &partition_type = input.input_table_types[result->columns[TaskFormat::PARTITION_CONSTANTS]];
	unordered_set<int32_t> field_ids;
	for (auto &field : StructType::GetChildTypes(partition_type)) {
		auto id = Value(field.first).DefaultTryCastAs(LogicalType::INTEGER);
		if (!id || IntegerValue::Get(*id) <= 0 || !field_ids.insert(IntegerValue::Get(*id)).second) {
			throw BinderException("iceberg_scan_tasks partition constant names must be distinct positive field IDs");
		}
		result->partition_ids.push_back(IntegerValue::Get(*id));
	}
	return std::move(result);
}

static Value TaskValue(DataChunk &input, const IcebergScanTasksBindData &bind, idx_t row, TaskFormat::Column column,
                       bool nullable = false) {
	auto value = input.GetValue(bind.columns[column], row);
	if (!nullable && value.IsNull()) {
		throw InvalidInputException(
		    "iceberg_scan_tasks input column '%s' cannot be NULL",
		    TaskFormat::Columns(LogicalType::STRUCT({}), LogicalType::STRUCT({}))[column].first.GetIdentifierName());
	}
	return value;
}

static unique_ptr<IcebergActiveTask> StartTask(ExecutionContext &context, const IcebergScanTasksBindData &bind,
                                               IcebergScanTasksGlobalState &global, IcebergScanTasksLocalState &local,
                                               DataChunk &input) {
	if (input.GetValue(bind.columns[TaskFormat::METADATA], local.row).IsNull()) {
		throw InvalidInputException("iceberg_scan_tasks metadata and schema_id cannot be NULL");
	}
	auto info = make_shared_ptr<IcebergTaskScanInfo>();
	info->execution = global.GetContext(context.client, bind, local.metadata_json.GetValue(local.row),
	                                    TaskValue(input, bind, local.row, TaskFormat::SCHEMA_ID),
	                                    TaskValue(input, bind, local.row, TaskFormat::SNAPSHOT_ID, true));
	auto path = TaskValue(input, bind, local.row, TaskFormat::FILE_PATH);
	auto format = TaskValue(input, bind, local.row, TaskFormat::FILE_FORMAT);
	auto size = TaskValue(input, bind, local.row, TaskFormat::FILE_SIZE);
	auto count = TaskValue(input, bind, local.row, TaskFormat::RECORD_COUNT);
	if (BigIntValue::Get(count) < 0) {
		throw InvalidInputException("iceberg_scan_tasks record_count cannot be negative");
	}
	auto spec = TaskValue(input, bind, local.row, TaskFormat::PARTITION_SPEC_ID);
	if (!info->execution->metadata.partition_specs.count(IntegerValue::Get(spec))) {
		throw InvalidInputException("iceberg_scan_tasks partition_spec_id is absent from the metadata");
	}
	auto first_row = TaskValue(input, bind, local.row, TaskFormat::FIRST_ROW_ID, true);
	auto sequence = TaskValue(input, bind, local.row, TaskFormat::SEQUENCE_NUMBER, true);
	info->file = TaskFormat::FileInfo(StringValue::Get(path), StringValue::Get(format), BigIntValue::Get(size),
	                                  first_row.IsNull() ? nullopt : optional<int64_t>(BigIntValue::Get(first_row)),
	                                  sequence.IsNull() ? nullopt : optional<int64_t>(BigIntValue::Get(sequence)));
	auto constants = TaskValue(input, bind, local.row, TaskFormat::PARTITION_CONSTANTS);
	auto &values = StructValue::GetChildren(constants);
	for (idx_t i = 0; i < values.size(); i++) {
		auto column = info->execution->schema->TryGetColumnByFieldId(bind.partition_ids[i]);
		if (!column || column->type != values[i].type()) {
			throw InvalidInputException("iceberg_scan_tasks partition constant %d does not match the selected schema",
			                            bind.partition_ids[i]);
		}
		info->partition_constants.emplace(bind.partition_ids[i], values[i]);
	}
	auto deletes = TaskValue(input, bind, local.row, TaskFormat::DELETE_FILES);
	info->delete_files = ListValue::GetChildren(deletes);

	auto result = make_uniq<IcebergActiveTask>();
	auto &entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
	    context.client, QualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, "parquet_scan"));
	result->function = *entry.functions.GetFunctionByArguments(context.client, {LogicalType::VARCHAR});
	result->function.function_info = info;
	result->function.get_multi_file_reader = IcebergTaskReader::CreateInstance;
	result->function.late_materialization = false;
	vector<Value> arguments {path};
	named_parameter_map_t parameters;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;
	TableFunctionRef ref;
	TableFunctionBindInput bind_input(arguments, parameters, input_types, input_names, nullptr, nullptr,
	                                  result->function, ref);
	vector<LogicalType> types;
	vector<Identifier> names;
	result->bind = result->function.bind(context.client, bind_input, types, names);
	vector<column_t> ids;
	for (idx_t i = 0; i < types.size(); i++) {
		ids.push_back(i);
	}
	TableFunctionInitInput init(result->bind.get(), ids, {}, nullptr);
	result->global = result->function.init_global(context.client, init);
	result->global->Cast<MultiFileGlobalState>().max_threads = 1;
	result->local = result->function.init_local(context, init, result->global.get());
	return result;
}

static OperatorResultType IcebergScanTasksFunction(ExecutionContext &context, TableFunctionInput &data,
                                                   DataChunk &input, DataChunk &output) {
	auto &bind = data.bind_data->Cast<IcebergScanTasksBindData>();
	auto &global = data.global_state->Cast<IcebergScanTasksGlobalState>();
	auto &local = data.local_state->Cast<IcebergScanTasksLocalState>();
	if (local.row == 0 && !local.active) {
		// A previous constant input leaves the cast result constant; start with writable flat storage.
		local.metadata_json.Initialize();
		VectorOperations::Cast(context.client, input.data[bind.columns[TaskFormat::METADATA]], local.metadata_json,
		                       input.size());
	}
	while (local.row < input.size()) {
		context.client.InterruptCheck();
		if (!local.active) {
			local.active = StartTask(context, bind, global, local, input);
		}
		auto &task = *local.active;
		TableFunctionInput scan_input(task.bind.get(), task.local.get(), task.global.get());
		output.Reset();
		task.function.function(context.client, scan_input, output);
		if (output.size()) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		local.active.reset();
		local.row++;
	}
	local.row = 0;
	return OperatorResultType::NEED_MORE_INPUT;
}

TableFunctionSet IcebergFunctions::GetIcebergScanTasksFunction() {
	TableFunction function("iceberg_scan_tasks", {LogicalType::TABLE}, nullptr, IcebergScanTasksBind);
	function.in_out_function = IcebergScanTasksFunction;
	function.init_global = [](ClientContext &, TableFunctionInitInput &) -> unique_ptr<GlobalTableFunctionState> {
		return make_uniq<IcebergScanTasksGlobalState>();
	};
	function.init_local = [](ExecutionContext &, TableFunctionInitInput &,
	                         GlobalTableFunctionState *) -> unique_ptr<LocalTableFunctionState> {
		return make_uniq<IcebergScanTasksLocalState>();
	};
	function.projection_pushdown = false;
	function.filter_pushdown = false;
	return TableFunctionSet(function);
}

} // namespace duckdb
