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

class SetCurrentViewVersionUpdate {
public:
	static SetCurrentViewVersionUpdate FromJSON(yyjson_val *obj) {
		SetCurrentViewVersionUpdate result;

		// Parse BaseUpdate fields
		result.base_update = BaseUpdate::FromJSON(obj);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}

		auto view_version_id_val = yyjson_obj_get(obj, "view-version-id");
		if (view_version_id_val) {
			result.view_version_id = yyjson_get_sint(view_version_id_val);
		} else {
			throw IOException("SetCurrentViewVersionUpdate required property 'view-version-id' is missing");
		}

		return result;
	}

public:
	BaseUpdate base_update;
	string action;
	int64_t view_version_id;
};
} // namespace rest_api_objects
} // namespace duckdb
