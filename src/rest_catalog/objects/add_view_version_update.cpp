
#include "rest_catalog/objects/add_view_version_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AddViewVersionUpdate::AddViewVersionUpdate() {
}

AddViewVersionUpdate AddViewVersionUpdate::FromJSON(JSONValue obj) {
	AddViewVersionUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AddViewVersionUpdate AddViewVersionUpdate::Copy() const {
	AddViewVersionUpdate res;
	res.base_update = base_update.Copy();
	res.view_version = view_version.Copy();
	return res;
}

string AddViewVersionUpdate::TryFromJSON(JSONValue obj) {
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
			    "AddViewVersionUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "add-view-version") {
			return "AddViewVersionUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "AddViewVersionUpdate required property 'action' is missing";
	}
	auto view_version_val = obj.GetMember("view-version");
	if (!view_version_val.IsValid()) {
		return "AddViewVersionUpdate required property 'view-version' is missing";
	} else {
		error = view_version.TryFromJSON(view_version_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void AddViewVersionUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: view-version
	auto view_version_json = view_version.ToJSON(writer);
	obj.Add("view-version", view_version_json);
}

JSONMutableValue AddViewVersionUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
