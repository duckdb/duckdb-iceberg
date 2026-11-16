
#include "rest_catalog/objects/update_namespace_properties_response.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

UpdateNamespacePropertiesResponse::UpdateNamespacePropertiesResponse() {
}

UpdateNamespacePropertiesResponse UpdateNamespacePropertiesResponse::FromJSON(JSONValue obj) {
	UpdateNamespacePropertiesResponse res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

UpdateNamespacePropertiesResponse UpdateNamespacePropertiesResponse::Copy() const {
	UpdateNamespacePropertiesResponse res;
	res.updated.reserve(updated.size());
	for (auto &item : updated) {
		res.updated.emplace_back(item);
	}
	res.removed.reserve(removed.size());
	for (auto &item : removed) {
		res.removed.emplace_back(item);
	}
	if (missing.has_value()) {
		res.missing.emplace();
		(*res.missing).reserve((*missing).size());
		for (auto &item : (*missing)) {
			(*res.missing).emplace_back(item);
		}
	}
	return res;
}

string UpdateNamespacePropertiesResponse::TryFromJSON(JSONValue obj) {
	string error;
	auto updated_val = obj.GetMember("updated");
	if (!updated_val.IsValid()) {
		return "UpdateNamespacePropertiesResponse required property 'updated' is missing";
	} else {
		if (updated_val.IsArray()) {
			updated_val.IterateArray([&](JSONValue updated_item_val) {
				if (!error.empty()) {
					return;
				}
				string updated_item;
				if (json_utils::IsString(updated_item_val)) {
					updated_item = json_utils::GetString(updated_item_val);
				} else {
					error = StringUtil::Format("UpdateNamespacePropertiesResponse property 'updated_item' is not of "
					                           "type 'string', found %s instead",
					                           json_utils::GetTypeDescription(updated_item_val).c_str());
					return;
				}
				updated.emplace_back(std::move(updated_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "UpdateNamespacePropertiesResponse property 'updated' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(updated_val).c_str());
		}
	}
	auto removed_val = obj.GetMember("removed");
	if (!removed_val.IsValid()) {
		return "UpdateNamespacePropertiesResponse required property 'removed' is missing";
	} else {
		if (removed_val.IsArray()) {
			removed_val.IterateArray([&](JSONValue removed_item_val) {
				if (!error.empty()) {
					return;
				}
				string removed_item;
				if (json_utils::IsString(removed_item_val)) {
					removed_item = json_utils::GetString(removed_item_val);
				} else {
					error = StringUtil::Format("UpdateNamespacePropertiesResponse property 'removed_item' is not of "
					                           "type 'string', found %s instead",
					                           json_utils::GetTypeDescription(removed_item_val).c_str());
					return;
				}
				removed.emplace_back(std::move(removed_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "UpdateNamespacePropertiesResponse property 'removed' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(removed_val).c_str());
		}
	}
	auto missing_val = obj.GetMember("missing");
	if (missing_val.IsValid()) {
		if (missing_val.IsNull()) {
			//! do nothing, property is explicitly nullable
		} else {
			vector<string> missing_tmp;
			if (missing_val.IsArray()) {
				missing_val.IterateArray([&](JSONValue missing_tmp_item_val) {
					if (!error.empty()) {
						return;
					}
					string missing_tmp_item;
					if (json_utils::IsString(missing_tmp_item_val)) {
						missing_tmp_item = json_utils::GetString(missing_tmp_item_val);
					} else {
						error = StringUtil::Format("UpdateNamespacePropertiesResponse property 'missing_tmp_item' is "
						                           "not of type 'string', found %s instead",
						                           json_utils::GetTypeDescription(missing_tmp_item_val).c_str());
						return;
					}
					missing_tmp.emplace_back(std::move(missing_tmp_item));
				});
				if (!error.empty()) {
					return error;
				}
			} else {
				return StringUtil::Format(
				    "UpdateNamespacePropertiesResponse property 'missing_tmp' is not of type 'array', found %s instead",
				    json_utils::GetTypeDescription(missing_val).c_str());
			}
			missing = std::move(missing_tmp);
		}
	}
	return "";
}

void UpdateNamespacePropertiesResponse::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: updated
	auto updated_json = writer.CreateArray();
	for (const auto &updated_json_item : updated) {
		auto updated_json_item_json = writer.CreateString(updated_json_item);
		updated_json.Append(updated_json_item_json);
	}
	obj.Add("updated", updated_json);

	// Serialize: removed
	auto removed_json = writer.CreateArray();
	for (const auto &removed_json_item : removed) {
		auto removed_json_item_json = writer.CreateString(removed_json_item);
		removed_json.Append(removed_json_item_json);
	}
	obj.Add("removed", removed_json);

	// Serialize: missing
	if (missing.has_value()) {
		auto &missing_value = *missing;
		auto missing_json = writer.CreateArray();
		for (const auto &missing_json_item : missing_value) {
			auto missing_json_item_json = writer.CreateString(missing_json_item);
			missing_json.Append(missing_json_item_json);
		}
		obj.Add("missing", missing_json);
	}
}

JSONMutableValue UpdateNamespacePropertiesResponse::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
