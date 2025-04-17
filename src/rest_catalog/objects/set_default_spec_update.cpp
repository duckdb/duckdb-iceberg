
#include "rest_catalog/objects/set_default_spec_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetDefaultSpecUpdate::SetDefaultSpecUpdate() {
}

SetDefaultSpecUpdate SetDefaultSpecUpdate::FromJSON(yyjson_val *obj) {
	SetDefaultSpecUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SetDefaultSpecUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto spec_id_val = yyjson_obj_get(obj, "spec-id");
	if (!spec_id_val) {
		return "SetDefaultSpecUpdate required property 'spec-id' is missing";
	} else {
		if (yyjson_is_int(spec_id_val)) {
			spec_id = yyjson_get_int(spec_id_val);
		} else {
			return StringUtil::Format(
			    "SetDefaultSpecUpdate property 'spec_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(spec_id_val));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "SetDefaultSpecUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
