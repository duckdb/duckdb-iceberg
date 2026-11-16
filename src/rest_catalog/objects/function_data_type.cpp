
#include "rest_catalog/objects/function_data_type.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionDataType::FunctionDataType() {
}
FunctionDataType::FunctionDataTypeOneOf1::FunctionDataTypeOneOf1() {
}

FunctionDataType::FunctionDataTypeOneOf1 FunctionDataType::FunctionDataTypeOneOf1::FromJSON(JSONValue obj) {
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

string FunctionDataType::FunctionDataTypeOneOf1::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsString(obj)) {
		value = json_utils::GetString(obj);
	} else {
		return StringUtil::Format("FunctionDataTypeOneOf1 property 'value' is not of type 'string', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue FunctionDataType::FunctionDataTypeOneOf1::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateString(value);
	return result;
}

FunctionDataType FunctionDataType::FromJSON(JSONValue obj) {
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

string FunctionDataType::TryFromJSON(JSONValue obj) {
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

JSONMutableValue FunctionDataType::ToJSON(JSONWriter &writer) const {
	if (function_data_type_one_of_1.has_value()) {
		return function_data_type_one_of_1->ToJSON(writer);
	} else if (function_list_type.has_value()) {
		return function_list_type->ToJSON(writer);
	} else if (function_map_type.has_value()) {
		return function_map_type->ToJSON(writer);
	} else if (function_struct_type.has_value()) {
		return function_struct_type->ToJSON(writer);
	}
	// No variant is active - return empty object
	return writer.CreateObject();
}

} // namespace rest_api_objects
} // namespace duckdb
