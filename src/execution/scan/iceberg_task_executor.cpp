#include "execution/scan/iceberg_task_executor.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "function/scan_planning/iceberg_row_filter.hpp"
#include "iceberg_logging.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

IcebergTaskExecutionContext::IcebergTaskExecutionContext(IcebergTableMetadata metadata_p, int32_t schema_id)
    : metadata(std::move(metadata_p)), schema(metadata.GetSchemaFromId(schema_id)) {
}

namespace {

struct IcebergTaskLocalState : public LocalTableFunctionState {
	unique_ptr<LocalTableFunctionState> scanner;
	//! Holds the reader's finalized rows, including predicate-only columns, for residual filtering.
	//! After filtering, only the requested output columns are referenced into the result chunk.
	DataChunk chunk;
	unique_ptr<ExpressionExecutor> filter;
	SelectionVector selection {STANDARD_VECTOR_SIZE};
};

struct IcebergTaskScanInfo : public TableFunctionInfo {
	shared_ptr<IcebergTaskExecutionContext> execution;
	vector<IcebergFileScanTask> tasks;
};

//! The ordinary reader owns mapping, read-ahead and chunk finalization. Only its planner-facing
//! binding and task lookup are replaced for an already materialized list of Iceberg file tasks.
struct IcebergTaskReader : public IcebergMultiFileReader {
	explicit IcebergTaskReader(shared_ptr<TableFunctionInfo> info) : IcebergMultiFileReader(std::move(info)) {
	}

	static unique_ptr<MultiFileReader> CreateInstance(const BoundTableFunction &function) {
		return make_uniq<IcebergTaskReader>(function.function_info);
	}

	shared_ptr<MultiFileList> CreateFileList(ClientContext &, const vector<string> &, const FileGlobInput &) override {
		auto &info = function_info->Cast<IcebergTaskScanInfo>();
		vector<OpenFileInfo> files;
		files.reserve(info.tasks.size());
		for (auto &task : info.tasks) {
			files.push_back(IcebergMultiFileReader::FileInfo(task));
		}
		return make_shared_ptr<SimpleMultiFileList>(std::move(files));
	}

	bool Bind(MultiFileOptions &, MultiFileList &, vector<LogicalType> &types, vector<Identifier> &names,
	          MultiFileReaderBindData &bind) override {
		auto &info = function_info->Cast<IcebergTaskScanInfo>();
		for (auto &column : info.execution->schema.columns) {
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
		auto file_index = reader_data.reader->file_list_idx.GetIndex();
		if (file_index >= info.tasks.size()) {
			throw InternalException("Missing Iceberg task for file-list index %llu", file_index);
		}
		auto &task = info.tasks[file_index];
		auto &execution = *info.execution;
		IcebergDeleteExecutionContext delete_context {context, FileSystem::GetFileSystem(context),
		                                              execution.metadata.location, execution.options,
		                                              execution.metadata};
		auto deletes = execution.deletes.ProcessDeletes(delete_context, task.original_file_path, task.delete_files);
		//! InitializeTaskReader resolves physical fields and fallback partition constants before
		//! CreateMapping evaluates these filters. A rejected constant skips the data read safely;
		//! testing task.partition_constants before resolving the file schema could discard real values.
		return InitializeTaskReader(reader_data, bind, columns, column_ids, filters, context, gstate,
		                            execution.metadata.GetSchemas(), execution.metadata.mappings, std::move(deletes),
		                            task.partition_constants);
	}
};

} // namespace

IcebergTaskExecutor::IcebergTaskExecutor(ClientContext &context, shared_ptr<IcebergTaskExecutionContext> execution,
                                         vector<IcebergFileScanTask> tasks, vector<ColumnIndex> column_indexes_p,
                                         const TableFilterSet &table_filters, unique_ptr<Expression> row_filter_p)
    : column_indexes(std::move(column_indexes_p)), row_filter(std::move(row_filter_p)), filters(table_filters.Copy()) {
	if (tasks.empty()) {
		throw InternalException("IcebergTaskExecutor requires at least one file task");
	}
	DUCKDB_LOG(context, IcebergLogType, "Iceberg scan tasks executor mode=%s task_count=%llu",
	           tasks.size() > 1 ? "multi_file" : "single_file", tasks.size());

	auto info = make_shared_ptr<IcebergTaskScanInfo>();
	info->execution = std::move(execution);
	vector<Value> paths;
	paths.reserve(tasks.size());
	for (auto &task : tasks) {
		paths.emplace_back(task.file_path);
	}
	info->tasks = std::move(tasks);
	auto path_list = Value::LIST(LogicalType::VARCHAR, std::move(paths));
	auto &entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
	    context, QualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, "parquet_scan"));
	function = *entry.functions.GetFunctionByArguments(context, {path_list.type()});
	function.function_info = info;
	function.get_multi_file_reader = IcebergTaskReader::CreateInstance;
	function.late_materialization = false;
	vector<Value> arguments;
	arguments.push_back(std::move(path_list));
	named_argument_map_t parameters;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;
	TableFunctionRef ref;
	BoundTableFunction bound_table_function(function);
	TableFunctionBindInput bind_input(arguments, parameters, input_types, input_names, nullptr, nullptr,
	                                  bound_table_function, ref);
	vector<LogicalType> types;
	vector<Identifier> names;
	bind = function.bind(context, bind_input, types, names);
	//! The reader resolves a virtual column id through the bind data's map, as iceberg_scan's reader does
	//! (IcebergTableSchemaVersion::GetScanFunction fills it the same way).
	auto &multi_file_bind = bind->Cast<MultiFileBindData>();
	MultiFileReader::GetVirtualColumns(context, multi_file_bind.reader_bind, multi_file_bind.virtual_columns);
	multi_file_bind.virtual_columns.emplace(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                                        TableColumn("file_row_number", LogicalType::BIGINT));
	for (idx_t i = 0; i < column_indexes.size(); i++) {
		output_columns.push_back(i);
	}
	if (row_filter) {
		vector<ColumnIndex> schema_columns;
		for (idx_t i = 0; i < types.size(); i++) {
			schema_columns.emplace_back(i);
		}
		bool always_false;
		//! Extract scan filters while column bindings still index the full table schema.
		auto schema_filters = IcebergRowFilter::TableFilters(context, *row_filter, schema_columns, always_false);
		//! Resolve bindings like ColumnBindingResolver: column_indexes maps scan-chunk positions to schema columns.
		//! Append missing predicate columns, then replace schema bindings with references into
		//! IcebergTaskLocalState::chunk. The residual ExpressionExecutor uses these positions; output_columns excludes
		//! the appended columns.
		ExpressionIterator::VisitExpressionClassMutable(
		    row_filter, ExpressionClass::BOUND_COLUMN_REF, [&](unique_ptr<Expression> &expr) {
			    auto column = expr->Cast<BoundColumnRefExpression>().Binding().column_index.GetIndex();
			    idx_t index = 0;
			    for (; index < column_indexes.size(); index++) {
				    if (column_indexes[index] == ColumnIndex(column)) {
					    break;
				    }
			    }
			    if (index == column_indexes.size()) {
				    column_indexes.emplace_back(column);
			    }
			    expr = make_uniq<BoundReferenceExpression>(expr->GetReturnType(), index);
		    });
		//! Incoming table filters already index column_indexes. Appending predicate-only columns
		//! preserves those positions. Remap descriptor filters and combine them with incoming filters.
		for (auto &entry : schema_filters) {
			for (idx_t i = 0; i < column_indexes.size(); i++) {
				if (column_indexes[i] == ColumnIndex(entry.GetIndex().GetIndex())) {
					filters->PushFilter(ProjectionIndex(i), entry.TakeFilter());
					break;
				}
			}
		}
	}
	for (auto &column : column_indexes) {
		if (column.IsVirtualColumn()) {
			auto entry = multi_file_bind.virtual_columns.find(column.GetPrimaryIndex());
			if (entry == multi_file_bind.virtual_columns.end()) {
				throw InternalException("iceberg_scan_tasks: virtual column %llu is not one the reader produces",
				                        column.GetPrimaryIndex());
			}
			scan_types.push_back(entry->second.type);
			continue;
		}
		scan_types.push_back(types[column.GetPrimaryIndex()]);
	}
	TableFunctionInitInput init(bind.get(), column_indexes, {}, filters.get());
	global = function.init_global(context, init);
}

IcebergTaskExecutor::~IcebergTaskExecutor() = default;

unique_ptr<LocalTableFunctionState> IcebergTaskExecutor::InitializeLocal(ExecutionContext &context) {
	auto result = make_uniq<IcebergTaskLocalState>();
	TableFunctionInitInput init(bind.get(), column_indexes, {}, filters.get());
	result->scanner = function.init_local(context, init, global.get());
	result->chunk.Initialize(context.client, scan_types);
	if (row_filter) {
		result->filter = make_uniq<ExpressionExecutor>(context.client, *row_filter);
	}
	return std::move(result);
}

bool IcebergTaskExecutor::Read(ClientContext &context, LocalTableFunctionState &local, DataChunk &output) {
	auto &state = local.Cast<IcebergTaskLocalState>();
	output.Reset();
	TableFunctionInput input(bind.get(), state.scanner.get(), global.get());
	while (true) {
		context.InterruptCheck();
		state.chunk.Reset();
		function.function(context, input, state.chunk);
		if (state.chunk.size() == 0) {
			return false;
		}
		if (state.filter) {
			auto count = state.filter->SelectExpression(state.chunk, state.selection);
			if (!count) {
				continue;
			}
			state.chunk.Slice(state.selection, count);
		}
		output.ReferenceColumns(state.chunk, output_columns);
		return true;
	}
}

} // namespace duckdb
