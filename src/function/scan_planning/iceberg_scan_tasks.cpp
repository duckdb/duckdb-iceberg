#include "function/iceberg_functions.hpp"

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "function/scan_planning/iceberg_scan_task_codec.hpp"
#include "execution/scan/iceberg_task_executor.hpp"
#include "function/scan_planning/iceberg_row_filter.hpp"

namespace duckdb {

using TaskCodec = IcebergScanTaskCodec;

struct IcebergScanTasksBindData : public TableFunctionData {
	TaskCodec::InputLayout layout;
	Value tasks;
};

struct IcebergScanTasksGlobalState : public GlobalTableFunctionState {
	explicit IcebergScanTasksGlobalState(vector<ColumnIndex> column_indexes_p)
	    : column_indexes(std::move(column_indexes_p)) {
	}

	const vector<ColumnIndex> column_indexes;
	mutex lock;
	idx_t next_task = 0;
	string metadata_json;
	Value schema_id;
	Value snapshot_id;
	shared_ptr<IcebergTaskExecutionContext> execution;
	shared_ptr<IcebergTaskExecutor> active;
	//! Cache bound filters by SQL text for this execution; all tasks share the same schema.
	unordered_map<string, unique_ptr<Expression>> row_filters;

	idx_t MaxThreads() const override {
		return MAX_THREADS;
	}

	// Called with lock held. Only the global state advances the input and publishes tasks.
	shared_ptr<IcebergTaskExecutor> GetTask(ClientContext &context, const IcebergScanTasksBindData &bind) {
		if (active || bind.tasks.IsNull()) {
			return active;
		}
		auto &tasks = ListValue::GetChildren(bind.tasks);
		if (next_task == tasks.size()) {
			return nullptr;
		}
		auto &descriptor = tasks[next_task];
		if (descriptor.IsNull()) {
			throw InvalidInputException("iceberg_scan_tasks requires non-NULL task structs");
		}
		auto schema = TaskCodec::ReadValue(descriptor, bind.layout, TaskCodec::SCHEMA_ID, true);
		auto snapshot = TaskCodec::ReadValue(descriptor, bind.layout, TaskCodec::SNAPSHOT_ID, true);
		auto metadata_value = TaskCodec::ReadValue(descriptor, bind.layout, TaskCodec::METADATA, true);
		if (metadata_value.IsNull() || schema.IsNull()) {
			throw InvalidInputException("iceberg_scan_tasks metadata and schema_id cannot be NULL");
		}
		auto json = metadata_value.CastAs(context, LogicalType::JSON());
		auto &text = StringValue::Get(json);
		if (execution) {
			if (text != metadata_json || !Value::NotDistinctFrom(schema, schema_id) ||
			    !Value::NotDistinctFrom(snapshot, snapshot_id)) {
				throw InvalidInputException(
				    "iceberg_scan_tasks requires one metadata document, schema ID, and snapshot ID");
			}
		} else {
			auto metadata = TaskCodec::ReadMetadata(text, IntegerValue::Get(schema), snapshot, bind.layout.schema_type);
			execution = make_shared_ptr<IcebergTaskExecutionContext>(std::move(metadata), IntegerValue::Get(schema));
			metadata_json = text;
			schema_id = schema;
			snapshot_id = snapshot;
		}
		auto task = TaskCodec::ReadTask(descriptor, bind.layout, execution->metadata, execution->schema);
		auto sql = TaskCodec::ReadValue(descriptor, bind.layout, TaskCodec::ROW_FILTER, true);
		unique_ptr<Expression> filter;
		if (!sql.IsNull()) {
			auto &text = StringValue::Get(sql);
			auto entry = row_filters.find(text);
			if (entry == row_filters.end()) {
				//! Parse and bind each distinct predicate once, even when many tasks carry it.
				entry = row_filters.emplace(text, IcebergRowFilter::Bind(context, text, bind.layout.schema_type)).first;
			}
			//! The executor rewrites column references to scan positions, so preserve the cached expression.
			filter = entry->second->Copy();
		}
		active = make_shared_ptr<IcebergTaskExecutor>(context, execution, std::move(task), column_indexes,
		                                              std::move(filter));
		next_task++;
		return active;
	}
};

struct IcebergScanTasksLocalState : public LocalTableFunctionState {
	// Release the worker's scanner before the shared task it references.
	shared_ptr<IcebergTaskExecutor> active;
	unique_ptr<LocalTableFunctionState> scanner;
};

static unique_ptr<FunctionData> IcebergScanTasksBind(ClientContext &, TableFunctionBindInput &input,
                                                     vector<LogicalType> &types, vector<Identifier> &names) {
	auto &tasks = input.inputs[0];
	if (tasks.type().id() != LogicalTypeId::LIST ||
	    ListType::GetChildType(tasks.type()).id() != LogicalTypeId::STRUCT) {
		throw BinderException("iceberg_scan_tasks requires a list of task structs");
	}
	auto result = make_uniq<IcebergScanTasksBindData>();
	result->layout = TaskCodec::BindInput(ListType::GetChildType(tasks.type()), types, names);
	result->tasks = tasks;
	return std::move(result);
}

static void IcebergScanTasksFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<IcebergScanTasksBindData>();
	auto &global = data.global_state->Cast<IcebergScanTasksGlobalState>();
	auto &local = data.local_state->Cast<IcebergScanTasksLocalState>();
	while (true) {
		context.InterruptCheck();
		if (!local.active) {
			{
				lock_guard<mutex> guard(global.lock);
				local.active = global.GetTask(context, bind);
			}
			if (!local.active) {
				return;
			}
			ThreadContext thread(context);
			ExecutionContext execution(context, thread, nullptr);
			local.scanner = local.active->InitializeLocal(execution);
		}
		if (local.active->Read(context, *local.scanner, output)) {
			return;
		}
		{
			lock_guard<mutex> guard(global.lock);
			// Other workers can still drain assigned row groups from this task. Their
			// references keep it alive while the global state advances to the next task.
			if (global.active == local.active) {
				global.active.reset();
			}
		}
		local.scanner.reset();
		local.active.reset();
	}
}

//! The file virtual columns the reader can produce for any task: the data file's path as the task
//! names it, and the row's position in that file -- the row id an Iceberg positional delete refers to.
static virtual_column_map_t IcebergScanTasksVirtualColumns(ClientContext &, optional_ptr<FunctionData>) {
	virtual_column_map_t result;
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR));
	result.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	               TableColumn("file_row_number", LogicalType::BIGINT));
	return result;
}

TableFunctionSet IcebergFunctions::GetIcebergScanTasksFunction() {
	TableFunction function("iceberg_scan_tasks", {LogicalType::ANY}, IcebergScanTasksFunction, IcebergScanTasksBind);
	function.get_virtual_columns = IcebergScanTasksVirtualColumns;
	function.init_global = [](ClientContext &, TableFunctionInitInput &input) -> unique_ptr<GlobalTableFunctionState> {
		return make_uniq<IcebergScanTasksGlobalState>(input.column_indexes);
	};
	function.init_local = [](ExecutionContext &, TableFunctionInitInput &,
	                         GlobalTableFunctionState *) -> unique_ptr<LocalTableFunctionState> {
		return make_uniq<IcebergScanTasksLocalState>();
	};
	function.projection_pushdown = true;
	function.filter_pushdown = false;
	function.order_preservation_type = OrderPreservationType::NO_ORDER;
	return TableFunctionSet(function);
}

} // namespace duckdb
