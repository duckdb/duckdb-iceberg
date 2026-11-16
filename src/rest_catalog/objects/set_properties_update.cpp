
#include "rest_catalog/objects/set_properties_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

SetPropertiesUpdate::SetPropertiesUpdate() {
}

SetPropertiesUpdate SetPropertiesUpdate::FromJSON(JSONValue obj) {
	SetPropertiesUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SetPropertiesUpdate SetPropertiesUpdate::Copy() const {
	SetPropertiesUpdate res;
	res.base_update = base_update.Copy();
	for (auto &entry : updates) {
		res.updates.emplace(entry.first, entry.second);
	}
	return res;
}

string SetPropertiesUpdate::TryFromJSON(JSONValue obj) {
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
			    "SetPropertiesUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "set-properties") {
			return "SetPropertiesUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "SetPropertiesUpdate required property 'action' is missing";
	}
	auto updates_val = obj.GetMember("updates");
	if (!updates_val.IsValid()) {
		return "SetPropertiesUpdate required property 'updates' is missing";
	} else {
		if (updates_val.IsObject()) {
			updates_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error = StringUtil::Format(
					    "SetPropertiesUpdate property 'tmp' is not of type 'string', found %s instead",
					    json_utils::GetTypeDescription(val).c_str());
					return;
				}
				updates.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "SetPropertiesUpdate property 'updates' is not of type 'object'";
		}
	}
	return "";
}

void SetPropertiesUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: updates
	auto updates_json = writer.CreateObject();
	for (const auto &[updates_json_key, updates_json_value] : updates) {
		auto updates_json_value_json = writer.CreateString(updates_json_value);
		updates_json.Add(updates_json_key, updates_json_value_json);
	}
	obj.Add("updates", updates_json);
}

JSONMutableValue SetPropertiesUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
