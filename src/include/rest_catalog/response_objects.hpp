#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

inline case_insensitive_map_t<string> parse_object_of_strings(yyjson_val *obj) {
	case_insensitive_map_t<string> result;
	size_t idx, max;
	yyjson_val *key, *val;
	yyjson_obj_foreach(obj, idx, max, key, val) {
		auto key_str = yyjson_get_str(key);
		auto val_str = yyjson_get_str(val);
		result[key_str] = val_str;
	}
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
