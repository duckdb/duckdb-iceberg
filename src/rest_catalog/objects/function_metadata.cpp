
#include "rest_catalog/objects/function_metadata.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionMetadata::FunctionMetadata() {
}

FunctionMetadata FunctionMetadata::FromJSON(JSONValue obj) {
	FunctionMetadata res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionMetadata FunctionMetadata::Copy() const {
	FunctionMetadata res;
	res.function_uuid = function_uuid;
	res.format_version = format_version;
	res.definitions.reserve(definitions.size());
	for (auto &item : definitions) {
		res.definitions.emplace_back(item.Copy());
	}
	res.definition_log.reserve(definition_log.size());
	for (auto &item : definition_log) {
		res.definition_log.emplace_back(item.Copy());
	}
	if (location.has_value()) {
		res.location.emplace();
		(*res.location) = (*location);
	}
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	if (secure.has_value()) {
		res.secure.emplace();
		(*res.secure) = (*secure);
	}
	if (_doc.has_value()) {
		res._doc.emplace();
		(*res._doc) = (*_doc);
	}
	return res;
}

string FunctionMetadata::TryFromJSON(JSONValue obj) {
	string error;
	auto function_uuid_val = obj.GetMember("function-uuid");
	if (!function_uuid_val.IsValid()) {
		return "FunctionMetadata required property 'function-uuid' is missing";
	} else {
		if (json_utils::IsString(function_uuid_val)) {
			function_uuid = json_utils::GetString(function_uuid_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'function_uuid' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(function_uuid_val).c_str());
		}
	}
	auto format_version_val = obj.GetMember("format-version");
	if (!format_version_val.IsValid()) {
		return "FunctionMetadata required property 'format-version' is missing";
	} else {
		if (json_utils::IsInteger(format_version_val)) {
			format_version = json_utils::GetSignedInteger(format_version_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'format_version' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(format_version_val).c_str());
		}
	}
	auto definitions_val = obj.GetMember("definitions");
	if (!definitions_val.IsValid()) {
		return "FunctionMetadata required property 'definitions' is missing";
	} else {
		if (definitions_val.IsArray()) {
			definitions_val.IterateArray([&](JSONValue definitions_item_val) {
				if (!error.empty()) {
					return;
				}
				FunctionDefinition definitions_item;
				error = definitions_item.TryFromJSON(definitions_item_val);
				if (!error.empty()) {
					return;
				}
				definitions.emplace_back(std::move(definitions_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'definitions' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(definitions_val).c_str());
		}
	}
	auto definition_log_val = obj.GetMember("definition-log");
	if (!definition_log_val.IsValid()) {
		return "FunctionMetadata required property 'definition-log' is missing";
	} else {
		if (definition_log_val.IsArray()) {
			definition_log_val.IterateArray([&](JSONValue definition_log_item_val) {
				if (!error.empty()) {
					return;
				}
				FunctionDefinitionLogEntry definition_log_item;
				error = definition_log_item.TryFromJSON(definition_log_item_val);
				if (!error.empty()) {
					return;
				}
				definition_log.emplace_back(std::move(definition_log_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'definition_log' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(definition_log_val).c_str());
		}
	}
	auto location_val = obj.GetMember("location");
	if (location_val.IsValid()) {
		string location_tmp;
		if (json_utils::IsString(location_val)) {
			location_tmp = json_utils::GetString(location_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'location_tmp' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(location_val).c_str());
		}
		location = std::move(location_tmp);
	}
	auto properties_val = obj.GetMember("properties");
	if (properties_val.IsValid()) {
		unordered_map<string, string> properties_tmp;
		if (properties_val.IsObject()) {
			properties_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error =
					    StringUtil::Format("FunctionMetadata property 'tmp' is not of type 'string', found %s instead",
					                       json_utils::GetTypeDescription(val).c_str());
					return;
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "FunctionMetadata property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	auto secure_val = obj.GetMember("secure");
	if (secure_val.IsValid()) {
		bool secure_tmp;
		if (json_utils::IsBoolean(secure_val)) {
			secure_tmp = json_utils::GetBoolean(secure_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'secure_tmp' is not of type 'boolean', found %s instead",
			    json_utils::GetTypeDescription(secure_val).c_str());
		}
		secure = std::move(secure_tmp);
	}
	auto _doc_val = obj.GetMember("doc");
	if (_doc_val.IsValid()) {
		string _doc_tmp;
		if (json_utils::IsString(_doc_val)) {
			_doc_tmp = json_utils::GetString(_doc_val);
		} else {
			return StringUtil::Format("FunctionMetadata property '_doc_tmp' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(_doc_val).c_str());
		}
		_doc = std::move(_doc_tmp);
	}
	return "";
}

void FunctionMetadata::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: function-uuid
	auto function_uuid_json = writer.CreateString(function_uuid);
	obj.Add("function-uuid", function_uuid_json);

	// Serialize: format-version
	auto format_version_json = writer.CreateSignedInteger(format_version);
	obj.Add("format-version", format_version_json);

	// Serialize: definitions
	auto definitions_json = writer.CreateArray();
	for (const auto &definitions_json_item : definitions) {
		auto definitions_json_item_json = definitions_json_item.ToJSON(writer);
		definitions_json.Append(definitions_json_item_json);
	}
	obj.Add("definitions", definitions_json);

	// Serialize: definition-log
	auto definition_log_json = writer.CreateArray();
	for (const auto &definition_log_json_item : definition_log) {
		auto definition_log_json_item_json = definition_log_json_item.ToJSON(writer);
		definition_log_json.Append(definition_log_json_item_json);
	}
	obj.Add("definition-log", definition_log_json);

	// Serialize: location
	if (location.has_value()) {
		auto &location_value = *location;
		auto location_json = writer.CreateString(location_value);
		obj.Add("location", location_json);
	}

	// Serialize: properties
	if (properties.has_value()) {
		auto &properties_value = *properties;
		auto properties_json = writer.CreateObject();
		for (const auto &[properties_json_key, properties_json_value] : properties_value) {
			auto properties_json_value_json = writer.CreateString(properties_json_value);
			properties_json.Add(properties_json_key, properties_json_value_json);
		}
		obj.Add("properties", properties_json);
	}

	// Serialize: secure
	if (secure.has_value()) {
		auto &secure_value = *secure;
		auto secure_json = writer.CreateBoolean(secure_value);
		obj.Add("secure", secure_json);
	}

	// Serialize: doc
	if (_doc.has_value()) {
		auto &_doc_value = *_doc;
		auto _doc_json = writer.CreateString(_doc_value);
		obj.Add("doc", _doc_json);
	}
}

JSONMutableValue FunctionMetadata::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
