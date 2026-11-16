
#include "rest_catalog/objects/set_current_view_version_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SetCurrentViewVersionUpdate::SetCurrentViewVersionUpdate() {
}

SetCurrentViewVersionUpdate SetCurrentViewVersionUpdate::FromJSON(JSONValue obj) {
	SetCurrentViewVersionUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetCurrentViewVersionUpdate SetCurrentViewVersionUpdate::Copy() const {
	SetCurrentViewVersionUpdate res;
	res.base_update = base_update.Copy();
	res.view_version_id = view_version_id;
	return res;
}

string SetCurrentViewVersionUpdate::TryFromJSON(JSONValue obj) {
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
			    "SetCurrentViewVersionUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "set-current-view-version") {
			return "SetCurrentViewVersionUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetCurrentViewVersionUpdate required property 'action' is missing";
	}
	auto view_version_id_val = obj.GetMember("view-version-id");
	if (!view_version_id_val.IsValid()) {
		return "SetCurrentViewVersionUpdate required property 'view-version-id' is missing";
	} else {
		if (json_utils::IsInteger(view_version_id_val)) {
			view_version_id = json_utils::GetSignedInteger(view_version_id_val);
		} else {
			return StringUtil::Format(
			    "SetCurrentViewVersionUpdate property 'view_version_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(view_version_id_val).c_str());
		}
	}
	return "";
}

void SetCurrentViewVersionUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: view-version-id
	auto view_version_id_json = writer.CreateSignedInteger(view_version_id);
	obj.Add("view-version-id", view_version_id_json);
}

JSONMutableValue SetCurrentViewVersionUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
