
#include "rest_catalog/objects/error_model.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ErrorModel::ErrorModel() {
}

ErrorModel ErrorModel::FromJSON(JSONValue obj) {
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

string ErrorModel::TryFromJSON(JSONValue obj) {
	string error;
	auto message_val = obj.GetMember("message");
	if (!message_val.IsValid()) {
		return "ErrorModel required property 'message' is missing";
	} else {
		if (json_utils::IsString(message_val)) {
			message = json_utils::GetString(message_val);
		} else {
			return StringUtil::Format("ErrorModel property 'message' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(message_val).c_str());
		}
	}
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "ErrorModel required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("ErrorModel property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
	}
	auto code_val = obj.GetMember("code");
	if (!code_val.IsValid()) {
		return "ErrorModel required property 'code' is missing";
	} else {
		if (json_utils::IsInteger(code_val)) {
			code = json_utils::GetSignedInteger(code_val);
		} else {
			return StringUtil::Format("ErrorModel property 'code' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(code_val).c_str());
		}
	}
	auto stack_val = obj.GetMember("stack");
	if (stack_val.IsValid()) {
		vector<string> stack_tmp;
		if (stack_val.IsArray()) {
			stack_val.IterateArray([&](JSONValue stack_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				string stack_tmp_item;
				if (json_utils::IsString(stack_tmp_item_val)) {
					stack_tmp_item = json_utils::GetString(stack_tmp_item_val);
				} else {
					error = StringUtil::Format(
					    "ErrorModel property 'stack_tmp_item' is not of type 'string', found %s instead",
					    json_utils::GetTypeDescription(stack_tmp_item_val).c_str());
					return;
				}
				stack_tmp.emplace_back(std::move(stack_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ErrorModel property 'stack_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(stack_val).c_str());
		}
		stack = std::move(stack_tmp);
	}
	return "";
}

void ErrorModel::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: message
	auto message_json = writer.CreateString(message);
	obj.Add("message", message_json);

	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: code
	auto code_json = writer.CreateSignedInteger(code);
	obj.Add("code", code_json);

	// Serialize: stack
	if (stack.has_value()) {
		auto &stack_value = *stack;
		auto stack_json = writer.CreateArray();
		for (const auto &stack_json_item : stack_value) {
			auto stack_json_item_json = writer.CreateString(stack_json_item);
			stack_json.Append(stack_json_item_json);
		}
		obj.Add("stack", stack_json);
	}
}

JSONMutableValue ErrorModel::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
