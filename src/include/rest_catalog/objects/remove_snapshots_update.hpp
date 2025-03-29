#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemoveSnapshotsUpdate {
public:
	static RemoveSnapshotsUpdate FromJSON(yyjson_val *obj) {
		RemoveSnapshotsUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto snapshot_ids_val = yyjson_obj_get(obj, "snapshot-ids");
		if (snapshot_ids_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(snapshot_ids_val, idx, max, val) {
				result.snapshot_ids.push_back(yyjson_get_sint(val));
			}
		} else {
			throw IOException("RemoveSnapshotsUpdate required property 'snapshot-ids' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	vector<int64_t> snapshot_ids;
};
} // namespace rest_api_objects
} // namespace duckdb
