
#include "rest_catalog/objects/function_map_type.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FunctionMapType::FunctionMapType() {
}

FunctionMapType FunctionMapType::FromJSON(yyjson_val *obj) {
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

string FunctionMapType::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "FunctionMapType required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format("FunctionMapType property 'type' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(type_val));
		}
		if (!yyjson_is_null(type_val) && type != "map") {
			return "FunctionMapType property 'type' does not match its required const value";
		}
	}
	auto key_val = yyjson_obj_get(obj, "key");
	if (!key_val) {
		return "FunctionMapType required property 'key' is missing";
	} else {
		key = make_uniq<FunctionDataType>();
		error = key->TryFromJSON(key_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_val = yyjson_obj_get(obj, "value");
	if (!value_val) {
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

void FunctionMapType::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: key
	yyjson_mut_val *key_val = key->ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "key", key_val);

	// Serialize: value
	yyjson_mut_val *value_val = value->ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "value", value_val);
}

yyjson_mut_val *FunctionMapType::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
