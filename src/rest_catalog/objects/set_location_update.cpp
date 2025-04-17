
#include "rest_catalog/objects/set_location_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetLocationUpdate::SetLocationUpdate() {
}

SetLocationUpdate SetLocationUpdate::FromJSON(yyjson_val *obj) {
	SetLocationUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SetLocationUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto location_val = yyjson_obj_get(obj, "location");
	if (!location_val) {
		return "SetLocationUpdate required property 'location' is missing";
	} else {
		if (yyjson_is_str(location_val)) {
			location = yyjson_get_str(location_val);
		} else {
			return StringUtil::Format(
			    "SetLocationUpdate property 'location' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(location_val));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format("SetLocationUpdate property 'action' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(action_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
