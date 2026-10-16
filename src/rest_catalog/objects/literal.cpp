
#include "rest_catalog/objects/literal.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Literal::Literal() {
}
Literal::Object2::Object2() {
}

Literal::Object2 Literal::Object2::FromJSON(JSONValue obj) {
	Object2 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Literal::Object2 Literal::Object2::Copy() const {
	Object2 res;
	res.type = type;
	res.value = value.Copy();
	if (data_type.has_value()) {
		res.data_type.emplace();
		(*res.data_type) = (*data_type).Copy();
	}
	return res;
}

string Literal::Object2::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "Object2 required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("Object2 property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "literal") {
			return "Object2 property 'type' does not match its required const value";
		}
	}
	auto value_val = obj.GetMember("value");
	if (!value_val.IsValid()) {
		return "Object2 required property 'value' is missing";
	} else {
		error = value.TryFromJSON(value_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto data_type_val = obj.GetMember("data-type");
	if (data_type_val.IsValid()) {
		PrimitiveType data_type_tmp;
		error = data_type_tmp.TryFromJSON(data_type_val);
		if (!error.empty()) {
			return error;
		}
		data_type = std::move(data_type_tmp);
	}
	return "";
}

void Literal::Object2::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: value
	auto value_json = value.ToJSON(writer);
	obj.Add("value", value_json);

	// Serialize: data-type
	if (data_type.has_value()) {
		auto &data_type_value = *data_type;
		auto data_type_json = data_type_value.ToJSON(writer);
		obj.Add("data-type", data_type_json);
	}
}

JSONMutableValue Literal::Object2::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

Literal Literal::FromJSON(JSONValue obj) {
	Literal res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Literal Literal::Copy() const {
	Literal res;
	if (primitive_type_value.has_value()) {
		res.primitive_type_value.emplace();
		(*res.primitive_type_value) = (*primitive_type_value).Copy();
	}
	if (object_2.has_value()) {
		res.object_2.emplace();
		(*res.object_2) = (*object_2).Copy();
	}
	return res;
}

string Literal::TryFromJSON(JSONValue obj) {
	string error;
	do {
		primitive_type_value.emplace();
		error = primitive_type_value->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			primitive_type_value = nullopt;
		}
		object_2.emplace();
		error = object_2->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			object_2 = nullopt;
		}
		return "Literal failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

JSONMutableValue Literal::ToJSON(JSONWriter &writer) const {
	if (primitive_type_value.has_value()) {
		return primitive_type_value->ToJSON(writer);
	} else if (object_2.has_value()) {
		return object_2->ToJSON(writer);
	}
	// No variant is active - return empty object
	return writer.CreateObject();
}

} // namespace rest_api_objects
} // namespace duckdb
