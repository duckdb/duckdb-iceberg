#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "iceberg_avro_multi_file_list.hpp"

namespace duckdb {

class AvroScan {
public:
	AvroScan(const string &path, ClientContext &context, shared_ptr<IcebergAvroScanInfo> avro_scan_info);

public:
	static unique_ptr<AvroScan> ScanManifestList(const IcebergSnapshot &snapshot, const IcebergTableMetadata &metadata,
	                                             ClientContext &context, const string &path);
	static unique_ptr<AvroScan> ScanManifest(const IcebergSnapshot &snapshot,
	                                         const vector<IcebergManifestFile> &manifest_files,
	                                         const IcebergOptions &options, FileSystem &fs, const string &iceberg_path,
	                                         const IcebergTableMetadata &metadata, ClientContext &context);

public:
	bool GetNext(DataChunk &chunk);
	void InitializeChunk(DataChunk &chunk);
	bool Finished() const;

public:
	string path;
	optional_ptr<TableFunction> avro_scan;
	ClientContext &context;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global_state;
	unique_ptr<LocalTableFunctionState> local_state;
	vector<LogicalType> return_types;
	vector<string> return_names;

	shared_ptr<IcebergAvroScanInfo> scan_info;
	bool finished = false;
};

} // namespace duckdb
