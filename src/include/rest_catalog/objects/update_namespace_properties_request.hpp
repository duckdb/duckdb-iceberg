#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UpdateNamespacePropertiesRequest {
public:
	static UpdateNamespacePropertiesRequest FromJSON(yyjson_val *obj) {
		UpdateNamespacePropertiesRequest result;
		auto removals_val = yyjson_obj_get(obj, "removals");
		if (removals_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(removals_val, idx, max, val) {
				result.removals.push_back(yyjson_get_str(val));
			}
		}
		auto updates_val = yyjson_obj_get(obj, "updates");
		if (updates_val) {
			result.updates = parse_object_of_strings(updates_val);
		}
		return result;
	}
public:
	vector<string> removals;
	ObjectOfStrings updates;
};

} // namespace rest_api_objects
} // namespace duckdb