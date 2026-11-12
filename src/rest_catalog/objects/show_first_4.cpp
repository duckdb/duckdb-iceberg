
#include "rest_catalog/objects/show_first_4.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ShowFirst4::ShowFirst4() {
}

ShowFirst4 ShowFirst4::FromJSON(JSONValue obj) {
	ShowFirst4 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

ShowFirst4 ShowFirst4::Copy() const {
	ShowFirst4 res;
	res.action = action;
	res.field_id = field_id;
	return res;
}

string ShowFirst4::TryFromJSON(JSONValue obj) {
	string error;
	auto action_val = obj.GetMember("action");
	if (!action_val.IsValid()) {
		return "ShowFirst4 required property 'action' is missing";
	} else {
		if (json_utils::IsString(action_val)) {
			action = json_utils::GetString(action_val);
		} else {
			return StringUtil::Format("ShowFirst4 property 'action' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(action_val).c_str());
		}
		if (!action_val.IsNull() && action != "show-first-4") {
			return "ShowFirst4 property 'action' does not match its required const value";
		}
	}
	auto field_id_val = obj.GetMember("field-id");
	if (!field_id_val.IsValid()) {
		return "ShowFirst4 required property 'field-id' is missing";
	} else {
		if (json_utils::IsInteger(field_id_val)) {
			field_id = json_utils::GetSignedInteger(field_id_val);
		} else {
			return StringUtil::Format("ShowFirst4 property 'field_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(field_id_val).c_str());
		}
	}
	return "";
}

void ShowFirst4::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: action
	auto action_json = writer.CreateString(action);
	obj.Add("action", action_json);

	// Serialize: field-id
	auto field_id_json = writer.CreateSignedInteger(field_id);
	obj.Add("field-id", field_id_json);
}

JSONMutableValue ShowFirst4::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
