#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ErrorModel {
public:
	static ErrorModel FromJSON(yyjson_val *obj) {
		ErrorModel result;
		auto message_val = yyjson_obj_get(obj, "message");
		if (message_val) {
			result.message = yyjson_get_str(message_val);
		}
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		auto code_val = yyjson_obj_get(obj, "code");
		if (code_val) {
			result.code = yyjson_get_sint(code_val);
		}
		auto stack_val = yyjson_obj_get(obj, "stack");
		if (stack_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(stack_val, idx, max, val) {
				result.stack.push_back(yyjson_get_str(val));
			}
		}
		return result;
	}
public:
	string message;
	string type;
	int64_t code;
	vector<string> stack;
};

} // namespace rest_api_objects
} // namespace duckdb