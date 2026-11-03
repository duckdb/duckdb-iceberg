#include "function/iceberg_functions.hpp"

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_options.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "function/scan_planning/iceberg_scan_task_codec.hpp"
#include "execution/scan/iceberg_task_executor.hpp"
#include "function/scan_planning/iceberg_row_filter.hpp"

namespace duckdb {

using TaskCodec = IcebergScanTaskCodec;

struct IcebergScanTasksBindData : public TableFunctionData {
	TaskCodec::InputLayout layout;
	Value tasks;
	vector<Identifier> names;
	vector<LogicalType> types;
	MultiFileOptions file_options;
	optional<idx_t> explicit_cardinality;
};

enum class IcebergScanTasksExecutionMode : uint8_t { UNSELECTED, SINGLE_FILE, MULTI_FILE };

enum class IcebergScanTasksListClass : uint8_t { ALL_NULL_FILTERS, COMMON_FILTER, DELETE_FILES, HETEROGENEOUS };

struct IcebergScanTasksListClassification {
	IcebergScanTasksListClass type;
	string common_filter;
};

struct IcebergScanTasksGlobalState : public GlobalTableFunctionState {
	IcebergScanTasksGlobalState(vector<ColumnIndex> column_indexes_p, optional_ptr<TableFilterSet> filters_p,
	                            bool multi_file_enabled_p)
	    : column_indexes(std::move(column_indexes_p)),
	      filters(filters_p ? filters_p->Copy() : make_uniq<TableFilterSet>()),
	      multi_file_enabled(multi_file_enabled_p) {
	}

	const vector<ColumnIndex> column_indexes;
	//! Keep finalized scan filters, including runtime join filters, alive across every file task.
	const unique_ptr<TableFilterSet> filters;
	const bool multi_file_enabled;
	mutex lock;
	idx_t next_task = 0;
	IcebergScanTasksExecutionMode execution_mode = IcebergScanTasksExecutionMode::UNSELECTED;
	string metadata_json;
	Value schema_id;
	Value snapshot_id;
	shared_ptr<IcebergTaskExecutionContext> execution;
	shared_ptr<IcebergTaskExecutor> active;
	//! Cache bound filters by SQL text for sequential execution; all tasks share the same schema.
	unordered_map<string, unique_ptr<Expression>> row_filters;

	idx_t MaxThreads() const override {
		return MAX_THREADS;
	}

	IcebergFileScanTask DecodeTask(ClientContext &context, const IcebergScanTasksBindData &bind,
	                               const Value &descriptor) {
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
		return TaskCodec::ReadTask(descriptor, bind.layout, execution->metadata, execution->schema);
	}

	IcebergScanTasksListClassification ClassifyTasks(ClientContext &context, const IcebergScanTasksBindData &bind,
	                                                 const vector<Value> &tasks) const {
		//! Delete processing retains per-file reader state for the executor lifetime. Keep every
		//! delete-bearing list on the established one-task-at-a-time path.
		for (auto &descriptor : tasks) {
			context.InterruptCheck();
			if (descriptor.IsNull()) {
				return {IcebergScanTasksListClass::HETEROGENEOUS, {}};
			}
			auto deletes = TaskCodec::ReadValue(descriptor, bind.layout, TaskCodec::DELETE_FILES, true);
			if (deletes.IsNull()) {
				return {IcebergScanTasksListClass::HETEROGENEOUS, {}};
			}
			if (!ListValue::GetChildren(deletes).empty()) {
				return {IcebergScanTasksListClass::DELETE_FILES, {}};
			}
		}

		bool have_filter = false;
		bool first_is_null = false;
		string common_filter;
		for (auto &descriptor : tasks) {
			context.InterruptCheck();
			auto filter = TaskCodec::ReadValue(descriptor, bind.layout, TaskCodec::ROW_FILTER, true);
			if (!have_filter) {
				have_filter = true;
				first_is_null = filter.IsNull();
				if (!first_is_null) {
					common_filter = StringValue::Get(filter);
				}
				continue;
			}
			if (filter.IsNull() != first_is_null) {
				return {IcebergScanTasksListClass::HETEROGENEOUS, {}};
			}
			if (!first_is_null && StringValue::Get(filter) != common_filter) {
				return {IcebergScanTasksListClass::HETEROGENEOUS, {}};
			}
		}
		if (first_is_null) {
			return {IcebergScanTasksListClass::ALL_NULL_FILTERS, {}};
		}
		return {IcebergScanTasksListClass::COMMON_FILTER, std::move(common_filter)};
	}

	unique_ptr<Expression> BindRowFilter(ClientContext &context, const IcebergScanTasksBindData &bind,
	                                     const string &sql) {
		auto entry = row_filters.find(sql);
		if (entry == row_filters.end()) {
			//! Parse and bind each distinct predicate once, even when many sequential tasks carry it.
			entry = row_filters.emplace(sql, IcebergRowFilter::Bind(context, sql, bind.layout.schema_type)).first;
		}
		//! The executor rewrites column references to scan positions, so preserve the cached expression.
		return entry->second->Copy();
	}

	// Called with lock held. Only global state advances the input and publishes executors.
	shared_ptr<IcebergTaskExecutor> GetTask(ClientContext &context, const IcebergScanTasksBindData &bind) {
		if (active || bind.tasks.IsNull()) {
			return active;
		}
		auto &tasks = ListValue::GetChildren(bind.tasks);
		if (next_task == tasks.size()) {
			return nullptr;
		}

		if (execution_mode == IcebergScanTasksExecutionMode::UNSELECTED) {
			if (!multi_file_enabled || tasks.size() < 2) {
				execution_mode = IcebergScanTasksExecutionMode::SINGLE_FILE;
			} else {
				auto classification = ClassifyTasks(context, bind, tasks);
				if (classification.type == IcebergScanTasksListClass::ALL_NULL_FILTERS ||
				    classification.type == IcebergScanTasksListClass::COMMON_FILTER) {
					execution_mode = IcebergScanTasksExecutionMode::MULTI_FILE;
					vector<IcebergFileScanTask> decoded_tasks;
					decoded_tasks.reserve(tasks.size());
					for (auto &descriptor : tasks) {
						context.InterruptCheck();
						decoded_tasks.push_back(DecodeTask(context, bind, descriptor));
					}
					unique_ptr<Expression> filter;
					if (classification.type == IcebergScanTasksListClass::COMMON_FILTER) {
						filter = IcebergRowFilter::Bind(context, classification.common_filter, bind.layout.schema_type);
					}
					next_task = tasks.size();
					active = make_shared_ptr<IcebergTaskExecutor>(context, execution, std::move(decoded_tasks),
					                                              column_indexes, *filters, std::move(filter));
					return active;
				}
				execution_mode = IcebergScanTasksExecutionMode::SINGLE_FILE;
			}
		}

		auto &descriptor = tasks[next_task];
		auto task = DecodeTask(context, bind, descriptor);
		auto sql = TaskCodec::ReadValue(descriptor, bind.layout, TaskCodec::ROW_FILTER, true);
		unique_ptr<Expression> filter;
		if (!sql.IsNull()) {
			filter = BindRowFilter(context, bind, StringValue::Get(sql));
		}
		vector<IcebergFileScanTask> single_task;
		single_task.push_back(std::move(task));
		active = make_shared_ptr<IcebergTaskExecutor>(context, execution, std::move(single_task), column_indexes,
		                                              *filters, std::move(filter));
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
	result->names = names;
	result->types = types;

	auto column_statistics = input.named_parameters.find("column_statistics");
	if (column_statistics != input.named_parameters.end()) {
		result->file_options.SetColumnStatistics(column_statistics->first, column_statistics->second);
		result->file_options.VerifyColumnStatistics(names, types);
	}
	auto explicit_cardinality = input.named_parameters.find("explicit_cardinality");
	if (explicit_cardinality != input.named_parameters.end() && !explicit_cardinality->second.IsNull()) {
		result->explicit_cardinality = UBigIntValue::Get(explicit_cardinality->second);
	}
	return std::move(result);
}

static unique_ptr<BaseStatistics> IcebergScanTasksStatistics(ClientContext &, const FunctionData *bind_data,
                                                             column_t column_index) {
	auto &bind = bind_data->Cast<IcebergScanTasksBindData>();
	if (column_index >= bind.names.size()) {
		return nullptr;
	}
	return bind.file_options.GetColumnStatistics(bind.names[column_index], bind.types[column_index]);
}

static unique_ptr<NodeStatistics> IcebergScanTasksCardinality(ClientContext &, const FunctionData *bind_data) {
	auto &bind = bind_data->Cast<IcebergScanTasksBindData>();
	if (bind.explicit_cardinality) {
		return make_uniq<NodeStatistics>(*bind.explicit_cardinality);
	}
	if (bind.tasks.IsNull()) {
		return make_uniq<NodeStatistics>(idx_t(0));
	}

	idx_t cardinality = 0;
	for (auto &descriptor : ListValue::GetChildren(bind.tasks)) {
		if (descriptor.IsNull()) {
			return nullptr;
		}
		auto record_count = TaskCodec::ReadValue(descriptor, bind.layout, TaskCodec::RECORD_COUNT, true);
		if (record_count.IsNull()) {
			return nullptr;
		}
		auto count_value = record_count.DefaultTryCastAs(LogicalType::UBIGINT, nullptr, true);
		if (!count_value) {
			// A count that is not a valid row count leaves the estimate unknown rather than failing the query
			return nullptr;
		}
		auto count = UBigIntValue::Get(*count_value);
		if (count > NumericLimits<idx_t>::Maximum() - cardinality) {
			return nullptr;
		}
		cardinality += count;
	}
	return make_uniq<NodeStatistics>(cardinality);
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
			// references keep it alive while global state advances to the next task.
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
	function.init_global = [](ClientContext &context,
	                          TableFunctionInitInput &input) -> unique_ptr<GlobalTableFunctionState> {
		Value setting;
		bool multi_file_enabled = DEFAULT_ENABLE_MULTI_FILE_SCAN_TASKS;
		if (context.TryGetCurrentSetting(ENABLE_MULTI_FILE_SCAN_TASKS_CONFIG_VARIABLE, setting) && !setting.IsNull()) {
			multi_file_enabled = setting.GetValue<bool>();
		}
		return make_uniq<IcebergScanTasksGlobalState>(input.column_indexes, input.filters, multi_file_enabled);
	};
	function.init_local = [](ExecutionContext &, TableFunctionInitInput &,
	                         GlobalTableFunctionState *) -> unique_ptr<LocalTableFunctionState> {
		return make_uniq<IcebergScanTasksLocalState>();
	};
	function.projection_pushdown = true;
	function.filter_pushdown = true;
	function.statistics = IcebergScanTasksStatistics;
	function.cardinality = IcebergScanTasksCardinality;
	function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("column_statistics", LogicalTypeId::ANY).Add("explicit_cardinality", LogicalType::UBIGINT);
	});
	function.order_preservation_type = OrderPreservationType::NO_ORDER;
	return TableFunctionSet(function);
}

} // namespace duckdb
