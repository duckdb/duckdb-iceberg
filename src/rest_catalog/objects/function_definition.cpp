
#include "rest_catalog/objects/function_definition.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FunctionDefinition::FunctionDefinition() {
}

FunctionDefinition FunctionDefinition::FromJSON(yyjson_val *obj) {
	FunctionDefinition res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionDefinition FunctionDefinition::Copy() const {
	FunctionDefinition res;
	res.definition_id = definition_id;
	res.parameters.reserve(parameters.size());
	for (auto &item : parameters) {
		res.parameters.emplace_back(item.Copy());
	}
	res.return_type = return_type ? make_uniq<FunctionDataType>(return_type->Copy()) : nullptr;
	res.versions.reserve(versions.size());
	for (auto &item : versions) {
		res.versions.emplace_back(item.Copy());
	}
	res.current_version_id = current_version_id;
	res.function_type = function_type;
	if (return_nullable.has_value()) {
		res.return_nullable.emplace();
		(*res.return_nullable) = (*return_nullable);
	}
	if (_doc.has_value()) {
		res._doc.emplace();
		(*res._doc) = (*_doc);
	}
	return res;
}

string FunctionDefinition::TryFromJSON(yyjson_val *obj) {
	string error;
	auto definition_id_val = yyjson_obj_get(obj, "definition-id");
	if (!definition_id_val) {
		return "FunctionDefinition required property 'definition-id' is missing";
	} else {
		if (yyjson_is_str(definition_id_val)) {
			definition_id = yyjson_get_str(definition_id_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'definition_id' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(definition_id_val));
		}
	}
	auto parameters_val = yyjson_obj_get(obj, "parameters");
	if (!parameters_val) {
		return "FunctionDefinition required property 'parameters' is missing";
	} else {
		if (yyjson_is_arr(parameters_val)) {
			size_t parameters_idx, parameters_max;
			yyjson_val *parameters_item_val;
			yyjson_arr_foreach(parameters_val, parameters_idx, parameters_max, parameters_item_val) {
				FunctionParameter parameters_item;
				error = parameters_item.TryFromJSON(parameters_item_val);
				if (!error.empty()) {
					return error;
				}
				parameters.emplace_back(std::move(parameters_item));
			}
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'parameters' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(parameters_val));
		}
	}
	auto return_type_val = yyjson_obj_get(obj, "return-type");
	if (!return_type_val) {
		return "FunctionDefinition required property 'return-type' is missing";
	} else {
		return_type = make_uniq<FunctionDataType>();
		error = return_type->TryFromJSON(return_type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto versions_val = yyjson_obj_get(obj, "versions");
	if (!versions_val) {
		return "FunctionDefinition required property 'versions' is missing";
	} else {
		if (yyjson_is_arr(versions_val)) {
			size_t versions_idx, versions_max;
			yyjson_val *versions_item_val;
			yyjson_arr_foreach(versions_val, versions_idx, versions_max, versions_item_val) {
				FunctionDefinitionVersion versions_item;
				error = versions_item.TryFromJSON(versions_item_val);
				if (!error.empty()) {
					return error;
				}
				versions.emplace_back(std::move(versions_item));
			}
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'versions' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(versions_val));
		}
	}
	auto current_version_id_val = yyjson_obj_get(obj, "current-version-id");
	if (!current_version_id_val) {
		return "FunctionDefinition required property 'current-version-id' is missing";
	} else {
		if (yyjson_is_int(current_version_id_val)) {
			current_version_id = yyjson_get_int(current_version_id_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'current_version_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(current_version_id_val));
		}
	}
	auto function_type_val = yyjson_obj_get(obj, "function-type");
	if (!function_type_val) {
		return "FunctionDefinition required property 'function-type' is missing";
	} else {
		if (yyjson_is_str(function_type_val)) {
			function_type = yyjson_get_str(function_type_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'function_type' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(function_type_val));
		}
	}
	auto return_nullable_val = yyjson_obj_get(obj, "return-nullable");
	if (return_nullable_val) {
		bool return_nullable_tmp;
		if (yyjson_is_bool(return_nullable_val)) {
			return_nullable_tmp = yyjson_get_bool(return_nullable_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property 'return_nullable_tmp' is not of type 'boolean', found '%s' instead",
			    yyjson_get_type_desc(return_nullable_val));
		}
		return_nullable = std::move(return_nullable_tmp);
	}
	auto _doc_val = yyjson_obj_get(obj, "doc");
	if (_doc_val) {
		string _doc_tmp;
		if (yyjson_is_str(_doc_val)) {
			_doc_tmp = yyjson_get_str(_doc_val);
		} else {
			return StringUtil::Format(
			    "FunctionDefinition property '_doc_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(_doc_val));
		}
		_doc = std::move(_doc_tmp);
	}
	return "";
}

void FunctionDefinition::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: definition-id
	yyjson_mut_obj_add_strcpy(doc, obj, "definition-id", definition_id.c_str());

	// Serialize: parameters
	yyjson_mut_val *parameters_arr = yyjson_mut_arr(doc);
	for (const auto &item : parameters) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(parameters_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "parameters", parameters_arr);

	// Serialize: return-type
	yyjson_mut_val *return_type_val = return_type->ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "return-type", return_type_val);

	// Serialize: versions
	yyjson_mut_val *versions_arr = yyjson_mut_arr(doc);
	for (const auto &item : versions) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(versions_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "versions", versions_arr);

	// Serialize: current-version-id
	yyjson_mut_obj_add_int(doc, obj, "current-version-id", current_version_id);

	// Serialize: function-type
	yyjson_mut_obj_add_strcpy(doc, obj, "function-type", function_type.c_str());

	// Serialize: return-nullable
	if (return_nullable.has_value()) {
		auto &return_nullable_value = *return_nullable;
		yyjson_mut_obj_add_bool(doc, obj, "return-nullable", return_nullable_value);
	}

	// Serialize: doc
	if (_doc.has_value()) {
		auto &_doc_value = *_doc;
		yyjson_mut_obj_add_strcpy(doc, obj, "doc", _doc_value.c_str());
	}
}

yyjson_mut_val *FunctionDefinition::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
