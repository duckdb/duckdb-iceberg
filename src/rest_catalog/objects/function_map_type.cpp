
#include "rest_catalog/objects/function_map_type.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionMapType::FunctionMapType() {
}

FunctionMapType FunctionMapType::FromJSON(JSONValue obj) {
	FunctionMapType res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionMapType FunctionMapType::Copy() const {
	FunctionMapType res;
	res.type = type;
	res.key = key ? make_uniq<FunctionDataType>(key->Copy()) : nullptr;
	res.value = value ? make_uniq<FunctionDataType>(value->Copy()) : nullptr;
	return res;
}

string FunctionMapType::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "FunctionMapType required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("FunctionMapType property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "map") {
			return "FunctionMapType property 'type' does not match its required const value";
		}
	}
	auto key_val = obj.GetMember("key");
	if (!key_val.IsValid()) {
		return "FunctionMapType required property 'key' is missing";
	} else {
		key = make_uniq<FunctionDataType>();
		error = key->TryFromJSON(key_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_val = obj.GetMember("value");
	if (!value_val.IsValid()) {
		return "FunctionMapType required property 'value' is missing";
	} else {
		value = make_uniq<FunctionDataType>();
		error = value->TryFromJSON(value_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void FunctionMapType::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: key
	auto key_json = key->ToJSON(writer);
	obj.Add("key", key_json);

	// Serialize: value
	auto value_json = value->ToJSON(writer);
	obj.Add("value", value_json);
}

JSONMutableValue FunctionMapType::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
