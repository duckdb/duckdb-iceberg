
#include "rest_catalog/objects/function_definition_version.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FunctionDefinitionVersion::FunctionDefinitionVersion() {
}

FunctionDefinitionVersion FunctionDefinitionVersion::FromJSON(yyjson_val *obj) {
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

string FunctionDefinitionVersion::TryFromJSON(yyjson_val *obj) {
	string error;
	auto version_id_val = yyjson_obj_get(obj, "version-id");
	if (!version_id_val) {
		return "FunctionDefinitionVersion required property 'version-id' is missing";
	} else {
		if (yyjson_is_int(version_id_val)) {
			version_id = yyjson_get_int(version_id_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersion property 'version_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(version_id_val));
		}
	}
	auto representations_val = yyjson_obj_get(obj, "representations");
	if (!representations_val) {
		return "FunctionDefinitionVersion required property 'representations' is missing";
	} else {
		if (yyjson_is_arr(representations_val)) {
			size_t representations_idx, representations_max;
			yyjson_val *representations_item_val;
			yyjson_arr_foreach(representations_val, representations_idx, representations_max,
			                   representations_item_val) {
				FunctionRepresentation representations_item;
				error = representations_item.TryFromJSON(representations_item_val);
				if (!error.empty()) {
					return error;
				}
				representations.emplace_back(std::move(representations_item));
			}
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersion property 'representations' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(representations_val));
		}
	}
	auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
	if (!timestamp_ms_val) {
		return "FunctionDefinitionVersion required property 'timestamp-ms' is missing";
	} else {
		if (yyjson_is_sint(timestamp_ms_val)) {
			timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		} else if (yyjson_is_uint(timestamp_ms_val)) {
			timestamp_ms = yyjson_get_uint(timestamp_ms_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersion property 'timestamp_ms' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(timestamp_ms_val));
		}
	}
	auto deterministic_val = yyjson_obj_get(obj, "deterministic");
	if (deterministic_val) {
		bool deterministic_tmp;
		if (yyjson_is_bool(deterministic_val)) {
			deterministic_tmp = yyjson_get_bool(deterministic_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersion property 'deterministic_tmp' is not of type 'boolean', found '%s' instead",
			    yyjson_get_type_desc(deterministic_val));
		}
		deterministic = std::move(deterministic_tmp);
	}
	auto on_null_input_val = yyjson_obj_get(obj, "on-null-input");
	if (on_null_input_val) {
		string on_null_input_tmp;
		if (yyjson_is_str(on_null_input_val)) {
			on_null_input_tmp = yyjson_get_str(on_null_input_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinitionVersion property 'on_null_input_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(on_null_input_val));
		}
		on_null_input = std::move(on_null_input_tmp);
	}
	return "";
}

void FunctionDefinitionVersion::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: version-id
	yyjson_mut_obj_add_int(doc, obj, "version-id", version_id);

	// Serialize: representations
	yyjson_mut_val *representations_arr = yyjson_mut_arr(doc);
	for (const auto &item : representations) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(representations_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "representations", representations_arr);

	// Serialize: timestamp-ms
	yyjson_mut_obj_add_sint(doc, obj, "timestamp-ms", timestamp_ms);

	// Serialize: deterministic
	if (deterministic.has_value()) {
		auto &deterministic_value = *deterministic;
		yyjson_mut_obj_add_bool(doc, obj, "deterministic", deterministic_value);
	}

	// Serialize: on-null-input
	if (on_null_input.has_value()) {
		auto &on_null_input_value = *on_null_input;
		yyjson_mut_obj_add_strcpy(doc, obj, "on-null-input", on_null_input_value.c_str());
	}
}

yyjson_mut_val *FunctionDefinitionVersion::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
