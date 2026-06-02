#pragma once

#include "duckdb/common/limits.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

struct IcebergFieldMapping {
public:
	static void ParseFieldMappings(yyjson_val *obj, vector<IcebergFieldMapping> &mappings, idx_t &mapping_index,
	                               idx_t parent_mapping_index);

public:
	//! field-id can be omitted for the root of a struct
	//! "Fields that exist in imported files but not in the Iceberg schema may omit field-id."
	int32_t field_id = NumericLimits<int32_t>::Maximum();
	//! "Fields which exist only in the Iceberg schema and not in imported data files may use an empty names list."
	case_insensitive_map_t<idx_t> field_mapping_indexes;
};

} // namespace duckdb
