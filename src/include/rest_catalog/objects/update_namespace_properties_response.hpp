#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UpdateNamespacePropertiesResponse {
public:
	static UpdateNamespacePropertiesResponse FromJSON(yyjson_val *obj) {
		UpdateNamespacePropertiesResponse result;

		auto updated_val = yyjson_obj_get(obj, "updated");
		if (updated_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(updated_val, idx, max, val) {
				result.updated.push_back(yyjson_get_str(val));
			}
		}
		else {
			throw IOException("UpdateNamespacePropertiesResponse required property 'updated' is missing");
		}

		auto removed_val = yyjson_obj_get(obj, "removed");
		if (removed_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(removed_val, idx, max, val) {
				result.removed.push_back(yyjson_get_str(val));
			}
		}
		else {
			throw IOException("UpdateNamespacePropertiesResponse required property 'removed' is missing");
		}

		auto missing_val = yyjson_obj_get(obj, "missing");
		if (missing_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(missing_val, idx, max, val) {
				result.missing.push_back(yyjson_get_str(val));
			}
		}

		return result;
	}

public:
	vector<string> updated;
	vector<string> removed;
	vector<string> missing;
};
} // namespace rest_api_objects
} // namespace duckdb