
#include "rest_catalog/objects/assign_uuidupdate.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AssignUUIDUpdate::AssignUUIDUpdate() {
}

AssignUUIDUpdate AssignUUIDUpdate::FromJSON(JSONValue obj) {
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

string AssignUUIDUpdate::TryFromJSON(JSONValue obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto action_refinement_val = obj.GetMember("action");
	if (action_refinement_val.IsValid()) {
		string action_refinement;
		if (json_utils::IsString(action_refinement_val)) {
			action_refinement = json_utils::GetString(action_refinement_val);
		} else {
			return StringUtil::Format(
			    "AssignUUIDUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "assign-uuid") {
			return "AssignUUIDUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "AssignUUIDUpdate required property 'action' is missing";
	}
	auto uuid_val = obj.GetMember("uuid");
	if (!uuid_val.IsValid()) {
		return "AssignUUIDUpdate required property 'uuid' is missing";
	} else {
		if (json_utils::IsString(uuid_val)) {
			uuid = json_utils::GetString(uuid_val);
		} else {
			return StringUtil::Format("AssignUUIDUpdate property 'uuid' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(uuid_val).c_str());
		}
	}
	return "";
}

void AssignUUIDUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: uuid
	auto uuid_json = writer.CreateString(uuid);
	obj.Add("uuid", uuid_json);
}

JSONMutableValue AssignUUIDUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
