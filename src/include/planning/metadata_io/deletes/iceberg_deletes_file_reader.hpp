//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/metadata_io/deletes/iceberg_deletes_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {

// Pass the already resolved open-file information to the delete scan.
struct IcebergDeleteScanInfo : public TableFunctionInfo {
public:
	IcebergDeleteScanInfo(vector<OpenFileInfo> file_infos, vector<MultiFileColumnDefinition> schema)
	    : file_infos(std::move(file_infos)), schema(std::move(schema)) {
	}

public:
	vector<OpenFileInfo> file_infos;
	//! Global schema shared by every Parquet file in this positional or equality delete batch.
	vector<MultiFileColumnDefinition> schema;
};

struct IcebergDeleteFileReader : public MultiFileReader {
	IcebergDeleteFileReader(shared_ptr<TableFunctionInfo> function_info);

	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	          vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;

	static unique_ptr<MultiFileReader> CreateInstance(const BoundTableFunction &table);

public:
	shared_ptr<TableFunctionInfo> function_info;
};

} // namespace duckdb
