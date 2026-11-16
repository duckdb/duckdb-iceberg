
#include "rest_catalog/objects/function_definition_log_entry.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionDefinitionLogEntry::FunctionDefinitionLogEntry() {
}

FunctionDefinitionLogEntry FunctionDefinitionLogEntry::FromJSON(JSONValue obj) {
	FunctionDefinitionLogEntry res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionDefinitionLogEntry FunctionDefinitionLogEntry::Copy() const {
	FunctionDefinitionLogEntry res;
	res.timestamp_ms = timestamp_ms;
	res.definition_versions.reserve(definition_versions.size());
	for (auto &item : definition_versions) {
		res.definition_versions.emplace_back(item.Copy());
	}
	return res;
}

string FunctionDefinitionLogEntry::TryFromJSON(JSONValue obj) {
	string error;
	auto timestamp_ms_val = obj.GetMember("timestamp-ms");
	if (!timestamp_ms_val.IsValid()) {
		return "FunctionDefinitionLogEntry required property 'timestamp-ms' is missing";
	} else {
		if (json_utils::IsInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetSignedInteger(timestamp_ms_val);
		} else if (json_utils::IsUnsignedInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetUnsignedInteger(timestamp_ms_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionLogEntry property 'timestamp_ms' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(timestamp_ms_val).c_str());
		}
	}
	auto definition_versions_val = obj.GetMember("definition-versions");
	if (!definition_versions_val.IsValid()) {
		return "FunctionDefinitionLogEntry required property 'definition-versions' is missing";
	} else {
		if (definition_versions_val.IsArray()) {
			definition_versions_val.IterateArray([&](JSONValue definition_versions_item_val) {
				if (!error.empty()) {
					return;
				}
				FunctionDefinitionVersionRef definition_versions_item;
				error = definition_versions_item.TryFromJSON(definition_versions_item_val);
				if (!error.empty()) {
					return;
				}
				definition_versions.emplace_back(std::move(definition_versions_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionLogEntry property 'definition_versions' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(definition_versions_val).c_str());
		}
	}
	return "";
}

void FunctionDefinitionLogEntry::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: timestamp-ms
	auto timestamp_ms_json = writer.CreateSignedInteger(timestamp_ms);
	obj.Add("timestamp-ms", timestamp_ms_json);

	// Serialize: definition-versions
	auto definition_versions_json = writer.CreateArray();
	for (const auto &definition_versions_json_item : definition_versions) {
		auto definition_versions_json_item_json = definition_versions_json_item.ToJSON(writer);
		definition_versions_json.Append(definition_versions_json_item_json);
	}
	obj.Add("definition-versions", definition_versions_json);
}

JSONMutableValue FunctionDefinitionLogEntry::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
