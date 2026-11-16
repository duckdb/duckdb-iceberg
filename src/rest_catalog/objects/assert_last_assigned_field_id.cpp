
#include "rest_catalog/objects/assert_last_assigned_field_id.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AssertLastAssignedFieldId::AssertLastAssignedFieldId() {
}

AssertLastAssignedFieldId AssertLastAssignedFieldId::FromJSON(JSONValue obj) {
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

string AssertLastAssignedFieldId::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "AssertLastAssignedFieldId required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format(
			    "AssertLastAssignedFieldId property 'type' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "assert-last-assigned-field-id") {
			return "AssertLastAssignedFieldId property 'type' does not match its required const value";
		}
	}
	auto last_assigned_field_id_val = obj.GetMember("last-assigned-field-id");
	if (!last_assigned_field_id_val.IsValid()) {
		return "AssertLastAssignedFieldId required property 'last-assigned-field-id' is missing";
	} else {
		if (json_utils::IsInteger(last_assigned_field_id_val)) {
			last_assigned_field_id = json_utils::GetSignedInteger(last_assigned_field_id_val);
		} else {
			return StringUtil::Format("AssertLastAssignedFieldId property 'last_assigned_field_id' is not of type "
			                          "'integer', found %s instead",
			                          json_utils::GetTypeDescription(last_assigned_field_id_val).c_str());
		}
	}
	return "";
}

void AssertLastAssignedFieldId::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: last-assigned-field-id
	auto last_assigned_field_id_json = writer.CreateSignedInteger(last_assigned_field_id);
	obj.Add("last-assigned-field-id", last_assigned_field_id_json);
}

JSONMutableValue AssertLastAssignedFieldId::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
