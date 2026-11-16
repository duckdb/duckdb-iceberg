
#include "rest_catalog/objects/commit_table_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CommitTableRequest::CommitTableRequest() {
}

CommitTableRequest CommitTableRequest::FromJSON(JSONValue obj) {
	CommitTableRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CommitTableRequest CommitTableRequest::Copy() const {
	CommitTableRequest res;
	res.requirements.reserve(requirements.size());
	for (auto &item : requirements) {
		res.requirements.emplace_back(item.Copy());
	}
	res.updates.reserve(updates.size());
	for (auto &item : updates) {
		res.updates.emplace_back(item.Copy());
	}
	if (identifier.has_value()) {
		res.identifier.emplace();
		(*res.identifier) = (*identifier).Copy();
	}
	return res;
}

string CommitTableRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto requirements_val = obj.GetMember("requirements");
	if (!requirements_val.IsValid()) {
		return "CommitTableRequest required property 'requirements' is missing";
	} else {
		if (requirements_val.IsArray()) {
			requirements_val.IterateArray([&](JSONValue requirements_item_val) {
				if (!error.empty()) {
					return;
				}
				TableRequirement requirements_item;
				error = requirements_item.TryFromJSON(requirements_item_val);
				if (!error.empty()) {
					return;
				}
				requirements.emplace_back(std::move(requirements_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "CommitTableRequest property 'requirements' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(requirements_val).c_str());
		}
	}
	auto updates_val = obj.GetMember("updates");
	if (!updates_val.IsValid()) {
		return "CommitTableRequest required property 'updates' is missing";
	} else {
		if (updates_val.IsArray()) {
			updates_val.IterateArray([&](JSONValue updates_item_val) {
				if (!error.empty()) {
					return;
				}
				TableUpdate updates_item;
				error = updates_item.TryFromJSON(updates_item_val);
				if (!error.empty()) {
					return;
				}
				updates.emplace_back(std::move(updates_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("CommitTableRequest property 'updates' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(updates_val).c_str());
		}
	}
	auto identifier_val = obj.GetMember("identifier");
	if (identifier_val.IsValid()) {
		TableIdentifier identifier_tmp;
		error = identifier_tmp.TryFromJSON(identifier_val);
		if (!error.empty()) {
			return error;
		}
		identifier = std::move(identifier_tmp);
	}
	return "";
}

void CommitTableRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: requirements
	auto requirements_json = writer.CreateArray();
	for (const auto &requirements_json_item : requirements) {
		auto requirements_json_item_json = requirements_json_item.ToJSON(writer);
		requirements_json.Append(requirements_json_item_json);
	}
	obj.Add("requirements", requirements_json);

	// Serialize: updates
	auto updates_json = writer.CreateArray();
	for (const auto &updates_json_item : updates) {
		auto updates_json_item_json = updates_json_item.ToJSON(writer);
		updates_json.Append(updates_json_item_json);
	}
	obj.Add("updates", updates_json);

	// Serialize: identifier
	if (identifier.has_value()) {
		auto &identifier_value = *identifier;
		auto identifier_json = identifier_value.ToJSON(writer);
		obj.Add("identifier", identifier_json);
	}
}

JSONMutableValue CommitTableRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
