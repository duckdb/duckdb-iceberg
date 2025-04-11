
#include "rest_catalog/objects/set_current_view_version_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetCurrentViewVersionUpdate::SetCurrentViewVersionUpdate() {
}

SetCurrentViewVersionUpdate SetCurrentViewVersionUpdate::FromJSON(yyjson_val *obj) {
	SetCurrentViewVersionUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SetCurrentViewVersionUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto view_version_id_val = yyjson_obj_get(obj, "view-version-id");
	if (!view_version_id_val) {
		return "SetCurrentViewVersionUpdate required property 'view-version-id' is missing";
	} else {
		view_version_id = yyjson_get_sint(view_version_id_val);
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		action = yyjson_get_str(action_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
