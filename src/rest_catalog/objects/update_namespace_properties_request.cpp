
#include "rest_catalog/objects/update_namespace_properties_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

UpdateNamespacePropertiesRequest::UpdateNamespacePropertiesRequest() {
}

UpdateNamespacePropertiesRequest UpdateNamespacePropertiesRequest::FromJSON(JSONValue obj) {
	UpdateNamespacePropertiesRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

UpdateNamespacePropertiesRequest UpdateNamespacePropertiesRequest::Copy() const {
	UpdateNamespacePropertiesRequest res;
	if (removals.has_value()) {
		res.removals.emplace();
		(*res.removals).reserve((*removals).size());
		for (auto &item : (*removals)) {
			(*res.removals).emplace_back(item);
		}
	}
	if (updates.has_value()) {
		res.updates.emplace();
		for (auto &entry : (*updates)) {
			(*res.updates).emplace(entry.first, entry.second);
		}
	}
	return res;
}

string UpdateNamespacePropertiesRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto removals_val = obj.GetMember("removals");
	if (removals_val.IsValid()) {
		vector<string> removals_tmp;
		if (removals_val.IsArray()) {
			removals_val.IterateArray([&](JSONValue removals_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				string removals_tmp_item;
				if (json_utils::IsString(removals_tmp_item_val)) {
					removals_tmp_item = json_utils::GetString(removals_tmp_item_val);
				} else {
					error = StringUtil::Format("UpdateNamespacePropertiesRequest property 'removals_tmp_item' is not "
					                           "of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(removals_tmp_item_val).c_str());
					return;
				}
				removals_tmp.emplace_back(std::move(removals_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "UpdateNamespacePropertiesRequest property 'removals_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(removals_val).c_str());
		}
		removals = std::move(removals_tmp);
	}
	auto updates_val = obj.GetMember("updates");
	if (updates_val.IsValid()) {
		unordered_map<string, string> updates_tmp;
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
					    "UpdateNamespacePropertiesRequest property 'tmp' is not of type 'string', found %s instead",
					    json_utils::GetTypeDescription(val).c_str());
					return;
				}
				updates_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "UpdateNamespacePropertiesRequest property 'updates_tmp' is not of type 'object'";
		}
		updates = std::move(updates_tmp);
	}
	return "";
}

void UpdateNamespacePropertiesRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: removals
	if (removals.has_value()) {
		auto &removals_value = *removals;
		auto removals_json = writer.CreateArray();
		for (const auto &removals_json_item : removals_value) {
			auto removals_json_item_json = writer.CreateString(removals_json_item);
			removals_json.Append(removals_json_item_json);
		}
		obj.Add("removals", removals_json);
	}

	// Serialize: updates
	if (updates.has_value()) {
		auto &updates_value = *updates;
		auto updates_json = writer.CreateObject();
		for (const auto &[updates_json_key, updates_json_value] : updates_value) {
			auto updates_json_value_json = writer.CreateString(updates_json_value);
			updates_json.Add(updates_json_key, updates_json_value_json);
		}
		obj.Add("updates", updates_json);
	}
}

JSONMutableValue UpdateNamespacePropertiesRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
