//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/metadata_io/iceberg_avro_multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"

namespace duckdb {

//! The function info of read_avro, extended with the Iceberg scan info that the multi-file reader needs
struct IcebergAvroMultiFileInfo : public TableFunctionMultiFileInfo {
	IcebergAvroMultiFileInfo(const TableFunctionMultiFileInfo &avro_info, shared_ptr<TableFunctionInfo> scan_info_p)
	    : TableFunctionMultiFileInfo(avro_info.function, avro_info.settings), scan_info(std::move(scan_info_p)) {
	}

	shared_ptr<TableFunctionInfo> scan_info;
};

struct IcebergAvroMultiFileReader : public MultiFileReader {
public:
	IcebergAvroMultiFileReader(shared_ptr<TableFunctionInfo> function_info) : function_info(std::move(function_info)) {
	}

public:
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	          vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;

	ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind_data,
	                                      const vector<MultiFileColumnDefinition> &global_columns,
	                                      const vector<ColumnIndex> &global_column_ids,
	                                      optional_ptr<TableFilterSet> table_filters, ClientContext &context,
	                                      MultiFileGlobalState &gstate) override;
	void FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data, BaseFileReader &reader,
	                   const MultiFileReaderData &reader_data, DataChunk &input_chunk, DataChunk &output_chunk,
	                   ExpressionExecutor &executor, optional_ptr<MultiFileReaderGlobalState> global_state) override;

public:
	static unique_ptr<MultiFileReader> CreateInstance(const BoundTableFunction &table);

private:
	shared_ptr<TableFunctionInfo> function_info;
};

} // namespace duckdb
