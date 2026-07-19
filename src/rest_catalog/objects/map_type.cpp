
#include "rest_catalog/objects/map_type.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

MapType::MapType() {
}

MapType MapType::FromJSON(yyjson_val *obj) {
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

string MapType::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "MapType required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format("MapType property 'type' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(type_val));
		}
		if (!yyjson_is_null(type_val) && type != "map") {
			return "MapType property 'type' does not match its required const value";
		}
	}
	auto key_id_val = yyjson_obj_get(obj, "key-id");
	if (!key_id_val) {
		return "MapType required property 'key-id' is missing";
	} else {
		if (yyjson_is_int(key_id_val)) {
			key_id = yyjson_get_int(key_id_val);
		} else {
			return StringUtil::Format("MapType property 'key_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(key_id_val));
		}
	}
	auto key_val = yyjson_obj_get(obj, "key");
	if (!key_val) {
		return "MapType required property 'key' is missing";
	} else {
		key = make_uniq<Type>();
		error = key->TryFromJSON(key_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_id_val = yyjson_obj_get(obj, "value-id");
	if (!value_id_val) {
		return "MapType required property 'value-id' is missing";
	} else {
		if (yyjson_is_int(value_id_val)) {
			value_id = yyjson_get_int(value_id_val);
		} else {
			return StringUtil::Format("MapType property 'value_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(value_id_val));
		}
	}
	auto value_val = yyjson_obj_get(obj, "value");
	if (!value_val) {
		return "MapType required property 'value' is missing";
	} else {
		value = make_uniq<Type>();
		error = value->TryFromJSON(value_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_required_val = yyjson_obj_get(obj, "value-required");
	if (!value_required_val) {
		return "MapType required property 'value-required' is missing";
	} else {
		if (yyjson_is_bool(value_required_val)) {
			value_required = yyjson_get_bool(value_required_val);
		} else {
			return StringUtil::Format("MapType property 'value_required' is not of type 'boolean', found '%s' instead",
			                          yyjson_get_type_desc(value_required_val));
		}
	}
	return "";
}

void MapType::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: key-id
	yyjson_mut_obj_add_int(doc, obj, "key-id", key_id);

	// Serialize: key
	yyjson_mut_val *key_val = key->ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "key", key_val);

	// Serialize: value-id
	yyjson_mut_obj_add_int(doc, obj, "value-id", value_id);

	// Serialize: value
	yyjson_mut_val *value_val = value->ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "value", value_val);

	// Serialize: value-required
	yyjson_mut_obj_add_bool(doc, obj, "value-required", value_required);
}

yyjson_mut_val *MapType::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
