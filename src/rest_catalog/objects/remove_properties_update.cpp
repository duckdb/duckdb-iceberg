
#include "rest_catalog/objects/remove_properties_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemovePropertiesUpdate::RemovePropertiesUpdate() {
}

RemovePropertiesUpdate RemovePropertiesUpdate::FromJSON(yyjson_val *obj) {
	RemovePropertiesUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string RemovePropertiesUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto removals_val = yyjson_obj_get(obj, "removals");
	if (!removals_val) {
		return "RemovePropertiesUpdate required property 'removals' is missing";
	} else {
		if (yyjson_is_arr(removals_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(removals_val, idx, max, val) {
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "RemovePropertiesUpdate property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				removals.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "RemovePropertiesUpdate property 'removals' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(removals_val));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "RemovePropertiesUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
