
#include "rest_catalog/objects/function_definition_log_entry.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FunctionDefinitionLogEntry::FunctionDefinitionLogEntry() {
}

FunctionDefinitionLogEntry FunctionDefinitionLogEntry::FromJSON(yyjson_val *obj) {
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

string FunctionDefinitionLogEntry::TryFromJSON(yyjson_val *obj) {
	string error;
	auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
	if (!timestamp_ms_val) {
		return "FunctionDefinitionLogEntry required property 'timestamp-ms' is missing";
	} else {
		if (yyjson_is_sint(timestamp_ms_val)) {
			timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		} else if (yyjson_is_uint(timestamp_ms_val)) {
			timestamp_ms = yyjson_get_uint(timestamp_ms_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionLogEntry property 'timestamp_ms' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(timestamp_ms_val));
		}
	}
	auto definition_versions_val = yyjson_obj_get(obj, "definition-versions");
	if (!definition_versions_val) {
		return "FunctionDefinitionLogEntry required property 'definition-versions' is missing";
	} else {
		if (yyjson_is_arr(definition_versions_val)) {
			size_t definition_versions_idx, definition_versions_max;
			yyjson_val *definition_versions_item_val;
			yyjson_arr_foreach(definition_versions_val, definition_versions_idx, definition_versions_max,
			                   definition_versions_item_val) {
				FunctionDefinitionVersionRef definition_versions_item;
				error = definition_versions_item.TryFromJSON(definition_versions_item_val);
				if (!error.empty()) {
					return error;
				}
				definition_versions.emplace_back(std::move(definition_versions_item));
			}
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionLogEntry property 'definition_versions' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(definition_versions_val));
		}
	}
	return "";
}

void FunctionDefinitionLogEntry::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: timestamp-ms
	yyjson_mut_obj_add_sint(doc, obj, "timestamp-ms", timestamp_ms);

	// Serialize: definition-versions
	yyjson_mut_val *definition_versions_arr = yyjson_mut_arr(doc);
	for (const auto &item : definition_versions) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(definition_versions_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "definition-versions", definition_versions_arr);
}

yyjson_mut_val *FunctionDefinitionLogEntry::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
