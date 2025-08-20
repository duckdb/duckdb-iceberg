
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

string AssertLastAssignedFieldId::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "AssertLastAssignedFieldId required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
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
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
