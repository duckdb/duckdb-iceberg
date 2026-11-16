
#include "rest_catalog/objects/set_location_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SetLocationUpdate::SetLocationUpdate() {
}

SetLocationUpdate SetLocationUpdate::FromJSON(JSONValue obj) {
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

string SetLocationUpdate::TryFromJSON(JSONValue obj) {
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
			    "SetLocationUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "set-location") {
			return "SetLocationUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetLocationUpdate required property 'action' is missing";
	}
	auto location_val = obj.GetMember("location");
	if (!location_val.IsValid()) {
		return "SetLocationUpdate required property 'location' is missing";
	} else {
		if (json_utils::IsString(location_val)) {
			location = json_utils::GetString(location_val);
		} else {
			return StringUtil::Format("SetLocationUpdate property 'location' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(location_val).c_str());
		}
	}
	return "";
}

void SetLocationUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: location
	auto location_json = writer.CreateString(location);
	obj.Add("location", location_json);
}

JSONMutableValue SetLocationUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
