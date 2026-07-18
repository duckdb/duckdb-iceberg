
#include "rest_catalog/objects/assign_uuidupdate.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssignUUIDUpdate::AssignUUIDUpdate() {
}

AssignUUIDUpdate AssignUUIDUpdate::FromJSON(yyjson_val *obj) {
	AssignUUIDUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AssignUUIDUpdate AssignUUIDUpdate::Copy() const {
	AssignUUIDUpdate res;
	res.base_update = base_update.Copy();
	res.uuid = uuid;
	return res;
}

string AssignUUIDUpdate::TryFromJSON(yyjson_val *obj) {
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
			    "AssignUUIDUpdate property 'action_refinement' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_refinement_val));
		}
		if (!yyjson_is_null(action_refinement_val) && action_refinement != "assign-uuid") {
			return "AssignUUIDUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "AssignUUIDUpdate required property 'action' is missing";
	}
	auto uuid_val = yyjson_obj_get(obj, "uuid");
	if (!uuid_val) {
		return "AssignUUIDUpdate required property 'uuid' is missing";
	} else {
		if (yyjson_is_str(uuid_val)) {
			uuid = yyjson_get_str(uuid_val);
		} else {
			return StringUtil::Format("AssignUUIDUpdate property 'uuid' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(uuid_val));
		}
	}
	return "";
}

void AssignUUIDUpdate::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(doc, obj);

	// Serialize: uuid
	yyjson_mut_obj_add_strcpy(doc, obj, "uuid", uuid.c_str());
}

yyjson_mut_val *AssignUUIDUpdate::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
