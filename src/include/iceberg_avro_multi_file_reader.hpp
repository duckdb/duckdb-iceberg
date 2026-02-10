//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_avro_multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {

struct IcebergAvroMultiFileReader : public MultiFileReader {
public:
	static constexpr column_t PARTITION_SPEC_ID_FIELD_ID = UINT64_C(10000000000000000000);
	static constexpr column_t SEQUENCE_NUMBER_FIELD_ID = UINT64_C(10000000000000000001);
	static constexpr column_t MANIFEST_FILE_INDEX_FIELD_ID = UINT64_C(10000000000000000002);

public:
	IcebergAvroMultiFileReader(shared_ptr<TableFunctionInfo> function_info) : function_info(std::move(function_info)) {
	}

public:
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types, vector<string> &names,
	          MultiFileReaderBindData &bind_data) override;
	unique_ptr<Expression>
	GetVirtualColumnExpression(ClientContext &context, MultiFileReaderData &reader_data,
	                           const vector<MultiFileColumnDefinition> &local_columns, idx_t &column_id,
	                           const LogicalType &type, MultiFileLocalIndex local_idx,
	                           optional_ptr<MultiFileColumnDefinition> &global_column_reference) override;

public:
	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);

private:
	shared_ptr<TableFunctionInfo> function_info;
};

} // namespace duckdb
