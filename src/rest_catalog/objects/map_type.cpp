
#include "rest_catalog/objects/map_type.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

MapType::MapType() {
}

MapType MapType::FromJSON(JSONValue obj) {
	MapType res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

MapType MapType::Copy() const {
	MapType res;
	res.type = type;
	res.key_id = key_id;
	res.key = key ? make_uniq<Type>(key->Copy()) : nullptr;
	res.value_id = value_id;
	res.value = value ? make_uniq<Type>(value->Copy()) : nullptr;
	res.value_required = value_required;
	return res;
}

string MapType::TryFromJSON(JSONValue obj) {
	string error;
	auto type_val = obj.GetMember("type");
	if (!type_val.IsValid()) {
		return "MapType required property 'type' is missing";
	} else {
		if (json_utils::IsString(type_val)) {
			type = json_utils::GetString(type_val);
		} else {
			return StringUtil::Format("MapType property 'type' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(type_val).c_str());
		}
		if (!type_val.IsNull() && type != "map") {
			return "MapType property 'type' does not match its required const value";
		}
	}
	auto key_id_val = obj.GetMember("key-id");
	if (!key_id_val.IsValid()) {
		return "MapType required property 'key-id' is missing";
	} else {
		if (json_utils::IsInteger(key_id_val)) {
			key_id = json_utils::GetSignedInteger(key_id_val);
		} else {
			return StringUtil::Format("MapType property 'key_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(key_id_val).c_str());
		}
	}
	auto key_val = obj.GetMember("key");
	if (!key_val.IsValid()) {
		return "MapType required property 'key' is missing";
	} else {
		key = make_uniq<Type>();
		error = key->TryFromJSON(key_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_id_val = obj.GetMember("value-id");
	if (!value_id_val.IsValid()) {
		return "MapType required property 'value-id' is missing";
	} else {
		if (json_utils::IsInteger(value_id_val)) {
			value_id = json_utils::GetSignedInteger(value_id_val);
		} else {
			return StringUtil::Format("MapType property 'value_id' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(value_id_val).c_str());
		}
	}
	auto value_val = obj.GetMember("value");
	if (!value_val.IsValid()) {
		return "MapType required property 'value' is missing";
	} else {
		value = make_uniq<Type>();
		error = value->TryFromJSON(value_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_required_val = obj.GetMember("value-required");
	if (!value_required_val.IsValid()) {
		return "MapType required property 'value-required' is missing";
	} else {
		if (json_utils::IsBoolean(value_required_val)) {
			value_required = json_utils::GetBoolean(value_required_val);
		} else {
			return StringUtil::Format("MapType property 'value_required' is not of type 'boolean', found %s instead",
			                          json_utils::GetTypeDescription(value_required_val).c_str());
		}
	}
	return "";
}

void MapType::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: type
	auto type_json = writer.CreateString(type);
	obj.Add("type", type_json);

	// Serialize: key-id
	auto key_id_json = writer.CreateSignedInteger(key_id);
	obj.Add("key-id", key_id_json);

	// Serialize: key
	auto key_json = key->ToJSON(writer);
	obj.Add("key", key_json);

	// Serialize: value-id
	auto value_id_json = writer.CreateSignedInteger(value_id);
	obj.Add("value-id", value_id_json);

	// Serialize: value
	auto value_json = value->ToJSON(writer);
	obj.Add("value", value_json);

	// Serialize: value-required
	auto value_required_json = writer.CreateBoolean(value_required);
	obj.Add("value-required", value_required_json);
}

JSONMutableValue MapType::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
