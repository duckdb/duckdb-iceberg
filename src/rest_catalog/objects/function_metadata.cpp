
#include "rest_catalog/objects/function_metadata.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FunctionMetadata::FunctionMetadata() {
}

FunctionMetadata FunctionMetadata::FromJSON(yyjson_val *obj) {
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

string FunctionMetadata::TryFromJSON(yyjson_val *obj) {
	string error;
	auto function_uuid_val = yyjson_obj_get(obj, "function-uuid");
	if (!function_uuid_val) {
		return "FunctionMetadata required property 'function-uuid' is missing";
	} else {
		if (yyjson_is_str(function_uuid_val)) {
			function_uuid = yyjson_get_str(function_uuid_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'function_uuid' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(function_uuid_val));
		}
	}
	auto format_version_val = yyjson_obj_get(obj, "format-version");
	if (!format_version_val) {
		return "FunctionMetadata required property 'format-version' is missing";
	} else {
		if (yyjson_is_int(format_version_val)) {
			format_version = yyjson_get_int(format_version_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'format_version' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(format_version_val));
		}
	}
	auto definitions_val = yyjson_obj_get(obj, "definitions");
	if (!definitions_val) {
		return "FunctionMetadata required property 'definitions' is missing";
	} else {
		if (yyjson_is_arr(definitions_val)) {
			size_t definitions_idx, definitions_max;
			yyjson_val *definitions_item_val;
			yyjson_arr_foreach(definitions_val, definitions_idx, definitions_max, definitions_item_val) {
				FunctionDefinition definitions_item;
				error = definitions_item.TryFromJSON(definitions_item_val);
				if (!error.empty()) {
					return error;
				}
				definitions.emplace_back(std::move(definitions_item));
			}
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'definitions' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(definitions_val));
		}
	}
	auto definition_log_val = yyjson_obj_get(obj, "definition-log");
	if (!definition_log_val) {
		return "FunctionMetadata required property 'definition-log' is missing";
	} else {
		if (yyjson_is_arr(definition_log_val)) {
			size_t definition_log_idx, definition_log_max;
			yyjson_val *definition_log_item_val;
			yyjson_arr_foreach(definition_log_val, definition_log_idx, definition_log_max, definition_log_item_val) {
				FunctionDefinitionLogEntry definition_log_item;
				error = definition_log_item.TryFromJSON(definition_log_item_val);
				if (!error.empty()) {
					return error;
				}
				definition_log.emplace_back(std::move(definition_log_item));
			}
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'definition_log' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(definition_log_val));
		}
	}
	auto location_val = yyjson_obj_get(obj, "location");
	if (location_val) {
		string location_tmp;
		if (yyjson_is_str(location_val)) {
			location_tmp = yyjson_get_str(location_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'location_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(location_val));
		}
		location = std::move(location_tmp);
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		case_insensitive_map_t<string> properties_tmp;
		if (yyjson_is_obj(properties_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(properties_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "FunctionMetadata property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			}
		} else {
			return "FunctionMetadata property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	auto secure_val = yyjson_obj_get(obj, "secure");
	if (secure_val) {
		bool secure_tmp;
		if (yyjson_is_bool(secure_val)) {
			secure_tmp = yyjson_get_bool(secure_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property 'secure_tmp' is not of type 'boolean', found '%s' instead",
			    yyjson_get_type_desc(secure_val));
		}
		secure = std::move(secure_tmp);
	}
	auto _doc_val = yyjson_obj_get(obj, "doc");
	if (_doc_val) {
		string _doc_tmp;
		if (yyjson_is_str(_doc_val)) {
			_doc_tmp = yyjson_get_str(_doc_val);
		} else {
			return StringUtil::Format(
			    "FunctionMetadata property '_doc_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(_doc_val));
		}
		_doc = std::move(_doc_tmp);
	}
	return "";
}

void FunctionMetadata::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: function-uuid
	yyjson_mut_obj_add_strcpy(doc, obj, "function-uuid", function_uuid.c_str());

	// Serialize: format-version
	yyjson_mut_obj_add_int(doc, obj, "format-version", format_version);

	// Serialize: definitions
	yyjson_mut_val *definitions_arr = yyjson_mut_arr(doc);
	for (const auto &item : definitions) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(definitions_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "definitions", definitions_arr);

	// Serialize: definition-log
	yyjson_mut_val *definition_log_arr = yyjson_mut_arr(doc);
	for (const auto &item : definition_log) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(definition_log_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "definition-log", definition_log_arr);

	// Serialize: location
	if (location.has_value()) {
		auto &location_value = *location;
		yyjson_mut_obj_add_strcpy(doc, obj, "location", location_value.c_str());
	}

	// Serialize: properties
	if (properties.has_value()) {
		auto &properties_value = *properties;
		yyjson_mut_val *properties_value_obj = yyjson_mut_obj(doc);
		for (const auto &it : properties_value) {
			auto &key = it.first;
			auto &value = it.second;
			auto key_ptr = unsafe_yyjson_mut_strncpy(doc, key.c_str(), strlen(key.c_str()));
			yyjson_mut_obj_add_strcpy(doc, properties_value_obj, key_ptr, value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "properties", properties_value_obj);
	}

	// Serialize: secure
	if (secure.has_value()) {
		auto &secure_value = *secure;
		yyjson_mut_obj_add_bool(doc, obj, "secure", secure_value);
	}

	// Serialize: doc
	if (_doc.has_value()) {
		auto &_doc_value = *_doc;
		yyjson_mut_obj_add_strcpy(doc, obj, "doc", _doc_value.c_str());
	}
}

yyjson_mut_val *FunctionMetadata::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
