#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/snapshot_reference.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetSnapshotRefUpdate {
public:
	static SetSnapshotRefUpdate FromJSON(yyjson_val *obj) {
		SetSnapshotRefUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto ref_name_val = yyjson_obj_get(obj, "ref-name");
		if (ref_name_val) {
			result.ref_name = yyjson_get_str(ref_name_val);
		}
		return result;
	}
public:
	string action;
	string ref_name;
};

} // namespace rest_api_objects
} // namespace duckdb