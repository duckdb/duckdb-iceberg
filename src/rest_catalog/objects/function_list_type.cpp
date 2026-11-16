
#include "rest_catalog/objects/function_list_type.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionListType::FunctionListType() {
}

FunctionListType FunctionListType::FromJSON(JSONValue obj) {
	FunctionListType res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionListType FunctionListType::Copy() const {
	FunctionListType res;
	res.type = type;
	res.element = element ? make_uniq<FunctionDataType>(element->Copy()) : nullptr;
	return res;
}

string FunctionListType::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "FunctionListType required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("FunctionListType property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "list") {
			return "FunctionListType property 'type' does not match its required const value";
		}
	}
	auto element_val = obj.GetMember("element");
	if (!element_val.IsValid()) {
		return "FunctionListType required property 'element' is missing";
	} else {
		element = make_uniq<FunctionDataType>();
		error = element->TryFromJSON(element_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void FunctionListType::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: element
	auto element_json = element->ToJSON(writer);
	obj.Add("element", element_json);
}

JSONMutableValue FunctionListType::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
