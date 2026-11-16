
#include "rest_catalog/objects/function_struct_type.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FunctionStructType::FunctionStructType() {
}

FunctionStructType FunctionStructType::FromJSON(JSONValue obj) {
	FunctionStructType res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FunctionStructType FunctionStructType::Copy() const {
	FunctionStructType res;
	res.type = type;
	res.fields.reserve(fields.size());
	for (auto &item : fields) {
		res.fields.emplace_back(item.Copy());
	}
	return res;
}

string FunctionStructType::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "FunctionStructType required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("FunctionStructType property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "struct") {
			return "FunctionStructType property 'type' does not match its required const value";
		}
	}
	auto fields_val = obj.GetMember("fields");
	if (!fields_val.IsValid()) {
		return "FunctionStructType required property 'fields' is missing";
	} else {
		if (fields_val.IsArray()) {
			fields_val.IterateArray([&](JSONValue fields_item_val) {
				if (!error.empty()) {
					return;
				}
				FunctionStructField fields_item;
				error = fields_item.TryFromJSON(fields_item_val);
				if (!error.empty()) {
					return;
				}
				fields.emplace_back(std::move(fields_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("FunctionStructType property 'fields' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(fields_val).c_str());
		}
	}
	return "";
}

void FunctionStructType::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: fields
	auto fields_json = writer.CreateArray();
	for (const auto &fields_json_item : fields) {
		auto fields_json_item_json = fields_json_item.ToJSON(writer);
		fields_json.Append(fields_json_item_json);
	}
	obj.Add("fields", fields_json);
}

JSONMutableValue FunctionStructType::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
