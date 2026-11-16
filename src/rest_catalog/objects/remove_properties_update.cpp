
#include "rest_catalog/objects/remove_properties_update.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

RemovePropertiesUpdate::RemovePropertiesUpdate() {
}

RemovePropertiesUpdate RemovePropertiesUpdate::FromJSON(JSONValue obj) {
	RemovePropertiesUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

RemovePropertiesUpdate RemovePropertiesUpdate::Copy() const {
	RemovePropertiesUpdate res;
	res.base_update = base_update.Copy();
	res.removals.reserve(removals.size());
	for (auto &item : removals) {
		res.removals.emplace_back(item);
	}
	return res;
}

string RemovePropertiesUpdate::TryFromJSON(JSONValue obj) {
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
			    "RemovePropertiesUpdate property 'action_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(action_refinement_val).c_str());
		}
		if (!action_refinement_val.IsNull() && action_refinement != "remove-properties") {
			return "RemovePropertiesUpdate property 'action_refinement' does not match its required const value";
		}
	} else {
		return "RemovePropertiesUpdate required property 'action' is missing";
	}
	auto removals_val = obj.GetMember("removals");
	if (!removals_val.IsValid()) {
		return "RemovePropertiesUpdate required property 'removals' is missing";
	} else {
		if (removals_val.IsArray()) {
			removals_val.IterateArray([&](JSONValue removals_item_val) {
				if (!error.empty()) {
					return;
				}
				string removals_item;
				if (json_utils::IsString(removals_item_val)) {
					removals_item = json_utils::GetString(removals_item_val);
				} else {
					error = StringUtil::Format(
					    "RemovePropertiesUpdate property 'removals_item' is not of type 'string', found %s instead",
					    json_utils::GetTypeDescription(removals_item_val).c_str());
					return;
				}
				removals.emplace_back(std::move(removals_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "RemovePropertiesUpdate property 'removals' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(removals_val).c_str());
		}
	}
	return "";
}

void RemovePropertiesUpdate::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: BaseUpdate
	base_update.PopulateJSON(writer, obj);

	// Serialize: removals
	auto removals_json = writer.CreateArray();
	for (const auto &removals_json_item : removals) {
		auto removals_json_item_json = writer.CreateString(removals_json_item);
		removals_json.Append(removals_json_item_json);
	}
	obj.Add("removals", removals_json);
}

JSONMutableValue RemovePropertiesUpdate::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
