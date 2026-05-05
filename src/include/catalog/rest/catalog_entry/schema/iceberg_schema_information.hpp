#pragma once
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {
struct IcebergSchemaInformation {
	case_insensitive_map_t<string> properties;
	bool properties_loaded = false;
};

} // namespace duckdb
