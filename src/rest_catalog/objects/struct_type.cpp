
#include "rest_catalog/objects/struct_type.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

StructType::StructType() {
}

StructType StructType::FromJSON(JSONValue obj) {
	StructType res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

StructType StructType::Copy() const {
	StructType res;
	res.type = type;
	res.fields.reserve(fields.size());
	for (auto &item : fields) {
		res.fields.emplace_back(item ? make_uniq<StructField>(item->Copy()) : nullptr);
	}
	return res;
}

string StructType::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "StructType required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("StructType property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "struct") {
			return "StructType property 'type' does not match its required const value";
		}
	}
	auto fields_val = obj.GetMember("fields");
	if (!fields_val.IsValid()) {
		return "StructType required property 'fields' is missing";
	} else {
		if (fields_val.IsArray()) {
			fields_val.IterateArray([&](JSONValue fields_item_val) {
				if (!error.empty()) {
					return;
				}
				auto fields_item_p = make_uniq<StructField>();
				auto &fields_item = *fields_item_p;
				error = fields_item.TryFromJSON(fields_item_val);
				if (!error.empty()) {
					return;
				}
				fields.emplace_back(std::move(fields_item_p));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("StructType property 'fields' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(fields_val).c_str());
		}
	}
	return "";
}

void StructType::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: fields
	auto fields_json = writer.CreateArray();
	for (const auto &fields_json_item : fields) {
		auto fields_json_item_json = fields_json_item->ToJSON(writer);
		fields_json.Append(fields_json_item_json);
	}
	obj.Add("fields", fields_json);
}

JSONMutableValue StructType::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
