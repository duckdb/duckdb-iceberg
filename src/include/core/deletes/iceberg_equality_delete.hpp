#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

struct IcebergEqualityDeleteFile {
public:
	IcebergEqualityDeleteFile(string file_path_p, vector<int32_t> equality_ids_p)
	    : file_path(std::move(file_path_p)), equality_ids(std::move(equality_ids_p)) {
	}
	IcebergEqualityDeleteFile(const IcebergEqualityDeleteFile &) = delete;
	IcebergEqualityDeleteFile &operator=(const IcebergEqualityDeleteFile &) = delete;

public:
	//! Path of the Parquet equality-delete file, retained for diagnostics.
	string file_path;
	//! Columns in equality_values follow this field-id order.
	vector<int32_t> equality_ids;
	DataChunk equality_values;
};

} // namespace duckdb
