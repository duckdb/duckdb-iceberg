
#include "rest_catalog/objects/error_model.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

ErrorModel ErrorModel::Copy() const {
	ErrorModel res;
	res.message = message;
	res.type = type;
	res.code = code;
	if (stack.has_value()) {
		res.stack.emplace();
		(*res.stack).reserve((*stack).size());
		for (auto &item : (*stack)) {
			(*res.stack).emplace_back(item);
		}
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
			return StringUtil::Format("ErrorModel property 'message' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(message_val));
		}
	}
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "ErrorModel required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format("ErrorModel property 'type' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(type_val));
		}
	}
	auto code_val = yyjson_obj_get(obj, "code");
	if (!code_val) {
		return "ErrorModel required property 'code' is missing";
	} else {
		if (yyjson_is_int(code_val)) {
			code = yyjson_get_int(code_val);
		} else {
			return StringUtil::Format("ErrorModel property 'code' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(code_val));
		}
	}
	auto stack_val = yyjson_obj_get(obj, "stack");
	if (stack_val) {
		vector<string> stack_tmp;
		if (yyjson_is_arr(stack_val)) {
			size_t stack_tmp_idx, stack_tmp_max;
			yyjson_val *stack_tmp_item_val;
			yyjson_arr_foreach(stack_val, stack_tmp_idx, stack_tmp_max, stack_tmp_item_val) {
				string stack_tmp_item;
				if (yyjson_is_str(stack_tmp_item_val)) {
					stack_tmp_item = yyjson_get_str(stack_tmp_item_val);
				} else {
					return StringUtil::Format(
					    "ErrorModel property 'stack_tmp_item' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(stack_tmp_item_val));
				}
				stack_tmp.emplace_back(std::move(stack_tmp_item));
			}
		} else {
			return StringUtil::Format("ErrorModel property 'stack_tmp' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(stack_val));
		}
		stack = std::move(stack_tmp);
	}
	return "";
}

void ErrorModel::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: message
	yyjson_mut_obj_add_strcpy(doc, obj, "message", message.c_str());

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: code
	yyjson_mut_obj_add_int(doc, obj, "code", code);

	// Serialize: stack
	if (stack.has_value()) {
		auto &stack_value = *stack;
		yyjson_mut_val *stack_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : stack_value) {
			yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
			yyjson_mut_arr_append(stack_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "stack", stack_value_arr);
	}
}

yyjson_mut_val *ErrorModel::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
