
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

class SetPropertiesUpdate {
public:
	SetPropertiesUpdate::SetPropertiesUpdate() {
	}

public:
	static SetPropertiesUpdate FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = base_base_update.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}

		auto updates_val = yyjson_obj_get(obj, "updates");
		if (!updates_val) {
		return "SetPropertiesUpdate required property 'updates' is missing");
		}
		result.updates = parse_object_of_strings(updates_val);

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
			;
		}
		return string();
	}

public:
	BaseUpdate base_update;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
