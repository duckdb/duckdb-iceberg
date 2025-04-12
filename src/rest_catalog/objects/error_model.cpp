
#include "rest_catalog/objects/error_model.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ErrorModel::ErrorModel() {
}

ErrorModel ErrorModel::FromJSON(yyjson_val *obj) {
	ErrorModel res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ErrorModel::TryFromJSON(yyjson_val *obj) {
	string error;
	auto message_val = yyjson_obj_get(obj, "message");
	if (!message_val) {
		return "ErrorModel required property 'message' is missing";
	} else {
		if (yyjson_is_str(message_val)) {
			message = yyjson_get_str(message_val);
		} else {
			return "ErrorModel property 'message' is not of type 'string'";
		}
	}
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "ErrorModel required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return "ErrorModel property 'type' is not of type 'string'";
		}
	}
	auto code_val = yyjson_obj_get(obj, "code");
	if (!code_val) {
		return "ErrorModel required property 'code' is missing";
	} else {
		if (yyjson_is_sint(code_val)) {
			code = yyjson_get_sint(code_val);
		} else {
			return "ErrorModel property 'code' is not of type 'integer'";
		}
	}
	auto stack_val = yyjson_obj_get(obj, "stack");
	if (stack_val) {
		has_stack = true;
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(stack_val, idx, max, val) {
			string tmp;
			if (yyjson_is_str(val)) {
				tmp = yyjson_get_str(val);
			} else {
				return "ErrorModel property 'tmp' is not of type 'string'";
			}
			stack.emplace_back(std::move(tmp));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
