
#include "rest_catalog/objects/function_data_type.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FunctionDataType::FunctionDataType() {
}
FunctionDataType::FunctionDataTypeOneOf1::FunctionDataTypeOneOf1() {
}

FunctionDataType::FunctionDataTypeOneOf1 FunctionDataType::FunctionDataTypeOneOf1::FromJSON(yyjson_val *obj) {
	FunctionDataTypeOneOf1 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionDataType::FunctionDataTypeOneOf1 FunctionDataType::FunctionDataTypeOneOf1::Copy() const {
	FunctionDataTypeOneOf1 res;
	res.value = value;
	return res;
}

string FunctionDataType::FunctionDataTypeOneOf1::TryFromJSON(yyjson_val *obj) {
	string error;
	if (yyjson_is_str(obj)) {
		value = yyjson_get_str(obj);
	} else {
		return StringUtil::Format("FunctionDataTypeOneOf1 property 'value' is not of type 'string', found '%s' instead",
		                          yyjson_get_type_desc(obj));
	}
	return "";
}

yyjson_mut_val *FunctionDataType::FunctionDataTypeOneOf1::ToJSON(yyjson_mut_doc *doc) const {
	return yyjson_mut_strcpy(doc, value.c_str());
}

FunctionDataType FunctionDataType::FromJSON(yyjson_val *obj) {
	FunctionDataType res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionDataType FunctionDataType::Copy() const {
	FunctionDataType res;
	if (function_data_type_one_of_1.has_value()) {
		res.function_data_type_one_of_1.emplace();
		(*res.function_data_type_one_of_1) = (*function_data_type_one_of_1).Copy();
	}
	if (function_list_type.has_value()) {
		res.function_list_type.emplace();
		(*res.function_list_type) = (*function_list_type).Copy();
	}
	if (function_map_type.has_value()) {
		res.function_map_type.emplace();
		(*res.function_map_type) = (*function_map_type).Copy();
	}
	if (function_struct_type.has_value()) {
		res.function_struct_type.emplace();
		(*res.function_struct_type) = (*function_struct_type).Copy();
	}
	return res;
}

string FunctionDataType::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		function_data_type_one_of_1.emplace();
		error = function_data_type_one_of_1->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			function_data_type_one_of_1 = nullopt;
		}
		function_list_type.emplace();
		error = function_list_type->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			function_list_type = nullopt;
		}
		function_map_type.emplace();
		error = function_map_type->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			function_map_type = nullopt;
		}
		function_struct_type.emplace();
		error = function_struct_type->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			function_struct_type = nullopt;
		}
		return "FunctionDataType failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

yyjson_mut_val *FunctionDataType::ToJSON(yyjson_mut_doc *doc) const {
	if (function_data_type_one_of_1.has_value()) {
		return function_data_type_one_of_1->ToJSON(doc);
	} else if (function_list_type.has_value()) {
		return function_list_type->ToJSON(doc);
	} else if (function_map_type.has_value()) {
		return function_map_type->ToJSON(doc);
	} else if (function_struct_type.has_value()) {
		return function_struct_type->ToJSON(doc);
	}
	// No variant is active - return empty object
	return yyjson_mut_obj(doc);
}

} // namespace rest_api_objects
} // namespace duckdb
