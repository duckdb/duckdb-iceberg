
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ErrorModel {
public:
	ErrorModel::ErrorModel() {
	}

public:
	static ErrorModel FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto message_val = yyjson_obj_get(obj, "message");
		if (!message_val) {
		return "ErrorModel required property 'message' is missing");
		}
		message = yyjson_get_str(message_val);

		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
		return "ErrorModel required property 'type' is missing");
		}
		type = yyjson_get_str(type_val);

		auto code_val = yyjson_obj_get(obj, "code");
		if (!code_val) {
		return "ErrorModel required property 'code' is missing");
		}
		code = yyjson_get_sint(code_val);

		auto stack_val = yyjson_obj_get(obj, "stack");
		if (stack_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(stack_val, idx, max, val) {

				auto tmp = yyjson_get_str(val);
				stack.push_back(tmp);
			}
		}

		return string();
	}

public:
public:
	int64_t code;
	string message;
	vector<string> stack;
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
