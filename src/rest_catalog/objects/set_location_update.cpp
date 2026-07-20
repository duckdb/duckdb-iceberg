
#include "rest_catalog/objects/set_location_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

SetLocationUpdate SetLocationUpdate::Copy() const {
	SetLocationUpdate res;
	res.base_update = base_update.Copy();
	res.location = location;
	return res;
}

string SetLocationUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto action_refinement_val = yyjson_obj_get(obj, "action");
	if (action_refinement_val) {
		string action_refinement;
		if (yyjson_is_str(action_refinement_val)) {
			action_refinement = yyjson_get_str(action_refinement_val);
		} else {
			return StringUtil::Format(
			    "SetLocationUpdate property 'action_refinement' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_refinement_val));
		}
		if (!yyjson_is_null(action_refinement_val) && action_refinement != "set-location") {
			return "SetLocationUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetLocationUpdate required property 'action' is missing";
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
	return "";
}

void SetLocationUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: location
	yyjson_mut_obj_add_strcpy(doc, obj, "location", location.c_str());
}

yyjson_mut_val *SetLocationUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
