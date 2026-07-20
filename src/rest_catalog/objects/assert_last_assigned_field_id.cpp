
#include "rest_catalog/objects/assert_last_assigned_field_id.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssertLastAssignedFieldId::AssertLastAssignedFieldId() {
}

AssertLastAssignedFieldId AssertLastAssignedFieldId::FromJSON(yyjson_val *obj) {
	AssertLastAssignedFieldId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AssertLastAssignedFieldId AssertLastAssignedFieldId::Copy() const {
	AssertLastAssignedFieldId res;
	res.type = type;
	res.last_assigned_field_id = last_assigned_field_id;
	return res;
}

string AssertLastAssignedFieldId::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "AssertLastAssignedFieldId required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format(
			    "AssertLastAssignedFieldId property 'type' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(type_val));
		}
		if (!yyjson_is_null(type_val) && type != "assert-last-assigned-field-id") {
			return "AssertLastAssignedFieldId property 'type' does not match its required const value";
		}
	}
	auto last_assigned_field_id_val = yyjson_obj_get(obj, "last-assigned-field-id");
	if (!last_assigned_field_id_val) {
		return "AssertLastAssignedFieldId required property 'last-assigned-field-id' is missing";
	} else {
		if (yyjson_is_int(last_assigned_field_id_val)) {
			last_assigned_field_id = yyjson_get_int(last_assigned_field_id_val);
		} else {
			return StringUtil::Format("AssertLastAssignedFieldId property 'last_assigned_field_id' is not of type "
			                          "'integer', found '%s' instead",
			                          yyjson_get_type_desc(last_assigned_field_id_val));
		}
	}
	return "";
}

void AssertLastAssignedFieldId::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: last-assigned-field-id
	yyjson_mut_obj_add_int(doc, obj, "last-assigned-field-id", last_assigned_field_id);
}

yyjson_mut_val *AssertLastAssignedFieldId::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
