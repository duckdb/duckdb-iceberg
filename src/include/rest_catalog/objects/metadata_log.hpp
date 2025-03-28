#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class MetadataLog {
public:
	static MetadataLog FromJSON(yyjson_val *obj) {
		MetadataLog result;

		return result;
	}

public:
};
} // namespace rest_api_objects
} // namespace duckdb