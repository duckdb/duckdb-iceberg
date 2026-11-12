
#include "rest_catalog/objects/literals.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Literals::Literals() {
}
Literals::Object5::Object5() {
}

Literals::Object5 Literals::Object5::FromJSON(JSONValue obj) {
	Object5 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Literals::Object5 Literals::Object5::Copy() const {
	Object5 res;
	res.type = type;
	res.values.reserve(values.size());
	for (auto &item : values) {
		res.values.emplace_back(item.Copy());
	}
	res.data_type = data_type.Copy();
	return res;
}

string Literals::Object5::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "Object5 required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("Object5 property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "literals") {
			return "Object5 property 'type' does not match its required const value";
		}
	}
	auto values_val = obj.GetMember("values");
	if (!values_val.IsValid()) {
		return "Object5 required property 'values' is missing";
	} else {
		if (values_val.IsArray()) {
			values_val.IterateArray([&](JSONValue values_item_val) {
				if (!error.empty()) {
					return;
				}
				PrimitiveTypeValue values_item;
				error = values_item.TryFromJSON(values_item_val);
				if (!error.empty()) {
					return;
				}
				values.emplace_back(std::move(values_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("Object5 property 'values' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(values_val).c_str());
		}
	}
	auto data_type_val = obj.GetMember("data-type");
	if (!data_type_val.IsValid()) {
		return "Object5 required property 'data-type' is missing";
	} else {
		error = data_type.TryFromJSON(data_type_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void Literals::Object5::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: values
	auto values_json = writer.CreateArray();
	for (const auto &values_json_item : values) {
		auto values_json_item_json = values_json_item.ToJSON(writer);
		values_json.Append(values_json_item_json);
	}
	obj.Add("values", values_json);

	// Serialize: data-type
	auto data_type_json = data_type.ToJSON(writer);
	obj.Add("data-type", data_type_json);
}

JSONMutableValue Literals::Object5::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}
Literals::LiteralsOneOf1::LiteralsOneOf1() {
}

Literals::LiteralsOneOf1 Literals::LiteralsOneOf1::FromJSON(JSONValue obj) {
	LiteralsOneOf1 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Literals::LiteralsOneOf1 Literals::LiteralsOneOf1::Copy() const {
	LiteralsOneOf1 res;
	res.value.reserve(value.size());
	for (auto &item : value) {
		res.value.emplace_back(item.Copy());
	}
	return res;
}

string Literals::LiteralsOneOf1::TryFromJSON(JSONValue obj) {
	string error;
	if (obj.IsArray()) {
		obj.IterateArray([&](JSONValue value_item_val) {
			if (!error.empty()) {
				return;
			}
			Literal value_item;
			error = value_item.TryFromJSON(value_item_val);
			if (!error.empty()) {
				return;
			}
			value.emplace_back(std::move(value_item));
		});
		if (!error.empty()) {
			return error;
		}
	} else {
		return StringUtil::Format("LiteralsOneOf1 property 'value' is not of type 'array', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue Literals::LiteralsOneOf1::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateArray();
	for (const auto &result_item : value) {
		auto result_item_json = result_item.ToJSON(writer);
		result.Append(result_item_json);
	}
	return result;
}

Literals Literals::FromJSON(JSONValue obj) {
	Literals res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Literals Literals::Copy() const {
	Literals res;
	if (literals_one_of_1.has_value()) {
		res.literals_one_of_1.emplace();
		(*res.literals_one_of_1) = (*literals_one_of_1).Copy();
	}
	if (object_5.has_value()) {
		res.object_5.emplace();
		(*res.object_5) = (*object_5).Copy();
	}
	return res;
}

string Literals::TryFromJSON(JSONValue obj) {
	string error;
	do {
		literals_one_of_1.emplace();
		error = literals_one_of_1->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			literals_one_of_1 = nullopt;
		}
		object_5.emplace();
		error = object_5->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			object_5 = nullopt;
		}
		return "Literals failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

JSONMutableValue Literals::ToJSON(JSONWriter &writer) const {
	if (literals_one_of_1.has_value()) {
		return literals_one_of_1->ToJSON(writer);
	} else if (object_5.has_value()) {
		return object_5->ToJSON(writer);
	}
	// No variant is active - return empty object
	return writer.CreateObject();
}

} // namespace rest_api_objects
} // namespace duckdb
