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
	string file_format;
	optional_idx footer_size;
	idx_t delete_count;
	idx_t file_size_bytes = 0;
	idx_t pos_max_value;
	idx_t pos_min_value;
	optional_idx content_size_in_bytes;
	optional_idx content_offset;
};

} // namespace duckdb
