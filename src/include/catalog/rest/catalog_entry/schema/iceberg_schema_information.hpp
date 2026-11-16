#pragma once
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {
struct IcebergSchemaInformation {
	unordered_map<string, string> properties;
	bool properties_loaded = false;
};

} // namespace duckdb
