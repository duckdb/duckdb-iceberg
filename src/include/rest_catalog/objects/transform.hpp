#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Transform {
public:
	static Transform FromJSON(yyjson_val *obj) {
		Transform result;
		result.value = yyjson_get_str(obj);
		return result;
	}

public:
	string value;
};
} // namespace rest_api_objects
} // namespace duckdb