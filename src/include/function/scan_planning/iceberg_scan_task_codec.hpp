#pragma once

#include "planning/scan_plan/iceberg_scan_task.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! Owns the SQL task contract: binding, validation, and conversion to/from typed tasks.
//! File opening and scan state belong to the reader, not this codec.
struct IcebergScanTaskCodec {
	enum Column : idx_t {
		FILE_PATH,
		FILE_FORMAT,
		FILE_SIZE,
		RECORD_COUNT,
		SEQUENCE_NUMBER,
		FIRST_ROW_ID,
		PARTITION_SPEC_ID,
		PARTITION_CONSTANTS,
		DELETE_FILES,
		SNAPSHOT_ID,
		SCHEMA_ID,
		METADATA,
		SCHEMA,
		ROW_FILTER,
		COLUMN_COUNT
	};

	static LogicalType DeleteFileType();
	static LogicalType SchemaType(const IcebergTableSchema &schema);
	static child_list_t<LogicalType> Columns(const LogicalType &partition_type, const LogicalType &schema_type);
	static IcebergDeleteFile ReadDeleteFile(const Value &descriptor);
	static Value WriteDeleteFile(const IcebergDeleteFile &file);

	//! Column mapping and schema information established without consuming input rows.
	struct InputLayout {
		vector<idx_t> columns;
		vector<int32_t> partition_ids;
		LogicalType schema_type;
	};
	static InputLayout BindInput(const LogicalType &task_type, vector<LogicalType> &types, vector<Identifier> &names);
	static Value ReadValue(const Value &input, const InputLayout &layout, Column column, bool nullable = false);
	static IcebergFileScanTask ReadTask(const Value &input, const InputLayout &layout,
	                                    const IcebergTableMetadata &metadata, const IcebergTableSchema &schema);
	static IcebergTableMetadata ReadMetadata(const string &text, int32_t schema_id, const Value &snapshot_id,
	                                         const LogicalType &schema_type);
	static void WriteTask(const IcebergFileScanTask &task, DataChunk &output, idx_t row);
	static void WriteContext(DataChunk &output, idx_t count, optional<int64_t> snapshot_id, int32_t schema_id,
	                         Vector &metadata);
};

} // namespace duckdb
