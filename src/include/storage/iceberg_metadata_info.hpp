//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/iceberg_metadata_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct IcebergDeleteFileInfo {
	string data_file_path;
	string file_name;
	optional_idx footer_size;
	idx_t delete_count;
	idx_t file_size_bytes = 0;
};

struct IcebergFileData {
	string path;
	idx_t file_size_in_bytes = 0;
	optional_idx footer_size;
};

struct IcebergFileListExtendedEntry {
	IcebergFileData file;
	IcebergDeleteFileInfo delete_file;
	idx_t row_count;
	idx_t delete_count = 0;
};

} // namespace duckdb
