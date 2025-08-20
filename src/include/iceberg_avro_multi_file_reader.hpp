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
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         FileGlobOptions options) override;

	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);
};

} // namespace duckdb
