#include "execution/scan/iceberg_task_executor.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "function/scan_planning/iceberg_row_filter.hpp"
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
	OpenFileInfo file;
	IcebergFileScanTask task;
};

//! The ordinary reader owns mapping and chunk finalization. Only its planner-facing
//! binding and task lookup are replaced for a materialized single-file task.
struct IcebergTaskReader : public IcebergMultiFileReader {
	explicit IcebergTaskReader(shared_ptr<TableFunctionInfo> info) : IcebergMultiFileReader(std::move(info)) {
	}

	static unique_ptr<MultiFileReader> CreateInstance(const BoundTableFunction &function) {
		return make_uniq<IcebergTaskReader>(function.function_info);
	}

	shared_ptr<MultiFileList> CreateFileList(ClientContext &, const vector<string> &, const FileGlobInput &) override {
		auto &info = function_info->Cast<IcebergTaskScanInfo>();
		return make_shared_ptr<SimpleMultiFileList>(vector<OpenFileInfo> {info.file});
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
		auto &execution = *info.execution;
		IcebergDeleteExecutionContext delete_context {context, FileSystem::GetFileSystem(context),
		                                              execution.metadata.location, execution.options,
		                                              execution.metadata};
		auto deletes =
		    execution.deletes.ProcessDeletes(delete_context, info.task.original_file_path, info.task.delete_files);
		return InitializeTaskReader(reader_data, bind, columns, column_ids, filters, context, gstate,
		                            execution.metadata.GetSchemas(), execution.metadata.mappings, std::move(deletes),
		                            info.task.partition_constants);
	}
};

} // namespace

IcebergTaskExecutor::IcebergTaskExecutor(ClientContext &context, shared_ptr<IcebergTaskExecutionContext> execution,
                                         IcebergFileScanTask task, vector<ColumnIndex> column_indexes_p,
                                         unique_ptr<Expression> row_filter_p)
    : column_indexes(std::move(column_indexes_p)), row_filter(std::move(row_filter_p)) {
	auto info = make_shared_ptr<IcebergTaskScanInfo>();
	info->execution = std::move(execution);
	info->file = IcebergMultiFileReader::FileInfo(task.file_path, task.file_format, task.file_size_in_bytes,
	                                              task.first_row_id, task.sequence_number);
	info->task = std::move(task);
	auto &entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
	    context, QualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, "parquet_scan"));
	function = *entry.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});
	function.function_info = info;
	function.get_multi_file_reader = IcebergTaskReader::CreateInstance;
	function.late_materialization = false;
	vector<Value> arguments {Value(info->file.path)};
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
		//! Remap the extracted filters' target columns to the same scan order for Parquet initialization.
		for (auto &entry : schema_filters) {
			for (idx_t i = 0; i < column_indexes.size(); i++) {
				if (column_indexes[i] == ColumnIndex(entry.GetIndex().GetIndex())) {
					filters.PushFilter(ProjectionIndex(i), entry.TakeFilter());
					break;
				}
			}
		}
	}
	for (auto &column : column_indexes) {
		scan_types.push_back(types[column.GetPrimaryIndex()]);
	}
	TableFunctionInitInput init(bind.get(), column_indexes, {}, &filters);
	global = function.init_global(context, init);
}

IcebergTaskExecutor::~IcebergTaskExecutor() = default;

unique_ptr<LocalTableFunctionState> IcebergTaskExecutor::InitializeLocal(ExecutionContext &context) {
	auto result = make_uniq<IcebergTaskLocalState>();
	TableFunctionInitInput init(bind.get(), column_indexes, {}, &filters);
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
