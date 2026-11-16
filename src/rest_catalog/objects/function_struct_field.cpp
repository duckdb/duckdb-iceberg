
#include "rest_catalog/objects/function_struct_field.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionStructField::FunctionStructField() {
}

FunctionStructField FunctionStructField::FromJSON(JSONValue obj) {
	FunctionStructField res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionStructField FunctionStructField::Copy() const {
	FunctionStructField res;
	res.name = name;
	res.type = type ? make_uniq<FunctionDataType>(type->Copy()) : nullptr;
	return res;
}

string FunctionStructField::TryFromJSON(JSONValue obj) {
	string error;
	auto name_val = obj.GetMember("name");
	if (!name_val.IsValid()) {
		return "FunctionStructField required property 'name' is missing";
	} else {
		if (json_utils::IsString(name_val)) {
			name = json_utils::GetString(name_val);
		} else {
			return StringUtil::Format("FunctionStructField property 'name' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(name_val).c_str());
		}
	}
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "FunctionStructField required property 'type' is missing";
	} else {
		type = make_uniq<FunctionDataType>();
		error = type->TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void FunctionStructField::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: name
	auto name_json = writer.CreateString(name);
	obj.Add("name", name_json);

	// Serialize: type
	auto type_json = type->ToJSON(writer);
	obj.Add("type", type_json);
}

JSONMutableValue FunctionStructField::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
