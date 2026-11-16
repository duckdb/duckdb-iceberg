
#include "rest_catalog/objects/function_definition_version.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionDefinitionVersion::FunctionDefinitionVersion() {
}

FunctionDefinitionVersion FunctionDefinitionVersion::FromJSON(JSONValue obj) {
	FunctionDefinitionVersion res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionDefinitionVersion FunctionDefinitionVersion::Copy() const {
	FunctionDefinitionVersion res;
	res.version_id = version_id;
	res.representations.reserve(representations.size());
	for (auto &item : representations) {
		res.representations.emplace_back(item.Copy());
	}
	res.timestamp_ms = timestamp_ms;
	if (deterministic.has_value()) {
		res.deterministic.emplace();
		(*res.deterministic) = (*deterministic);
	}
	if (on_null_input.has_value()) {
		res.on_null_input.emplace();
		(*res.on_null_input) = (*on_null_input);
	}
	return res;
}

string FunctionDefinitionVersion::TryFromJSON(JSONValue obj) {
	string error;
	auto version_id_val = obj.GetMember("version-id");
	if (!version_id_val.IsValid()) {
		return "FunctionDefinitionVersion required property 'version-id' is missing";
	} else {
		if (json_utils::IsInteger(version_id_val)) {
			version_id = json_utils::GetSignedInteger(version_id_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersion property 'version_id' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(version_id_val).c_str());
		}
	}
	auto representations_val = obj.GetMember("representations");
	if (!representations_val.IsValid()) {
		return "FunctionDefinitionVersion required property 'representations' is missing";
	} else {
		if (representations_val.IsArray()) {
			representations_val.IterateArray([&](JSONValue representations_item_val) {
				if (!error.empty()) {
					return;
				}
				FunctionRepresentation representations_item;
				error = representations_item.TryFromJSON(representations_item_val);
				if (!error.empty()) {
					return;
				}
				representations.emplace_back(std::move(representations_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersion property 'representations' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(representations_val).c_str());
		}
	}
	auto timestamp_ms_val = obj.GetMember("timestamp-ms");
	if (!timestamp_ms_val.IsValid()) {
		return "FunctionDefinitionVersion required property 'timestamp-ms' is missing";
	} else {
		if (json_utils::IsInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetSignedInteger(timestamp_ms_val);
		} else if (json_utils::IsUnsignedInteger(timestamp_ms_val)) {
			timestamp_ms = json_utils::GetUnsignedInteger(timestamp_ms_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersion property 'timestamp_ms' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(timestamp_ms_val).c_str());
		}
	}
	auto deterministic_val = obj.GetMember("deterministic");
	if (deterministic_val.IsValid()) {
		bool deterministic_tmp;
		if (json_utils::IsBoolean(deterministic_val)) {
			deterministic_tmp = json_utils::GetBoolean(deterministic_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersion property 'deterministic_tmp' is not of type 'boolean', found %s instead",
			    json_utils::GetTypeDescription(deterministic_val).c_str());
		}
		deterministic = std::move(deterministic_tmp);
	}
	auto on_null_input_val = obj.GetMember("on-null-input");
	if (on_null_input_val.IsValid()) {
		string on_null_input_tmp;
		if (json_utils::IsString(on_null_input_val)) {
			on_null_input_tmp = json_utils::GetString(on_null_input_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersion property 'on_null_input_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(on_null_input_val).c_str());
		}
		on_null_input = std::move(on_null_input_tmp);
	}
	return "";
}

void FunctionDefinitionVersion::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: version-id
	auto version_id_json = writer.CreateSignedInteger(version_id);
	obj.Add("version-id", version_id_json);

	// Serialize: representations
	auto representations_json = writer.CreateArray();
	for (const auto &representations_json_item : representations) {
		auto representations_json_item_json = representations_json_item.ToJSON(writer);
		representations_json.Append(representations_json_item_json);
	}
	obj.Add("representations", representations_json);

	// Serialize: timestamp-ms
	auto timestamp_ms_json = writer.CreateSignedInteger(timestamp_ms);
	obj.Add("timestamp-ms", timestamp_ms_json);

	// Serialize: deterministic
	if (deterministic.has_value()) {
		auto &deterministic_value = *deterministic;
		auto deterministic_json = writer.CreateBoolean(deterministic_value);
		obj.Add("deterministic", deterministic_json);
	}

	// Serialize: on-null-input
	if (on_null_input.has_value()) {
		auto &on_null_input_value = *on_null_input;
		auto on_null_input_json = writer.CreateString(on_null_input_value);
		obj.Add("on-null-input", on_null_input_json);
	}
}

JSONMutableValue FunctionDefinitionVersion::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
