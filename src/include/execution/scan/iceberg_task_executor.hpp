#pragma once

#include "duckdb/function/table_function.hpp"
#include "iceberg_options.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "planning/deletes/iceberg_delete_file_scanner.hpp"
#include "planning/scan_plan/iceberg_scan_task.hpp"

namespace duckdb {

//! Query-owned metadata and delete caches shared by all tasks and workers.
//! Schema references remain valid for the lifetime of this immovable context.
struct IcebergTaskExecutionContext {
	IcebergTaskExecutionContext(IcebergTableMetadata metadata, int32_t schema_id);
	IcebergTaskExecutionContext(const IcebergTaskExecutionContext &) = delete;
	IcebergTaskExecutionContext &operator=(const IcebergTaskExecutionContext &) = delete;

	const IcebergTableMetadata metadata;
	const IcebergTableSchema &schema;
	const IcebergOptions options;
	IcebergDeleteExecutionState deletes;
};

//! Executes one already-decoded file task. No SQL task layout or planner is needed.
//! Workers share the executor and each own a separate local scanner.
class IcebergTaskExecutor {
public:
	IcebergTaskExecutor(ClientContext &context, shared_ptr<IcebergTaskExecutionContext> execution,
	                    IcebergFileScanTask task, vector<ColumnIndex> column_indexes,
	                    unique_ptr<Expression> row_filter);
	~IcebergTaskExecutor();

	//! Creates a private scanner that claims work from the shared Parquet scan.
	unique_ptr<LocalTableFunctionState> InitializeLocal(ExecutionContext &context);
	//! Produces one chunk, or false when this worker has exhausted its scanner.
	bool Read(ClientContext &context, LocalTableFunctionState &local, DataChunk &output);

private:
	//! Reverse destruction order releases the global scanner before bind data and
	//! the function info that owns the task and shared execution context.
	TableFunction function;
	unique_ptr<FunctionData> bind;
	vector<ColumnIndex> column_indexes;
	vector<column_t> output_columns;
	vector<LogicalType> scan_types;
	unique_ptr<Expression> row_filter;
	TableFilterSet filters;
	unique_ptr<GlobalTableFunctionState> global;
};

} // namespace duckdb
