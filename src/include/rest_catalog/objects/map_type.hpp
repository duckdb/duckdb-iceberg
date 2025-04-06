
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class MapType {
public:
	MapType() {
	}

public:
	static MapType FromJSON(yyjson_val *obj) {
		MapType res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
			return "MapType required property 'type' is missing";
		} else {
			type = yyjson_get_str(type_val);
		}

		auto key_id_val = yyjson_obj_get(obj, "key_id");
		if (!key_id_val) {
			return "MapType required property 'key_id' is missing";
		} else {
			key_id = yyjson_get_sint(key_id_val);
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

		auto value_id_val = yyjson_obj_get(obj, "value_id");
		if (!value_id_val) {
			return "MapType required property 'value_id' is missing";
		} else {
			value_id = yyjson_get_sint(value_id_val);
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

		auto value_required_val = yyjson_obj_get(obj, "value_required");
		if (!value_required_val) {
			return "MapType required property 'value_required' is missing";
		} else {
			value_required = yyjson_get_bool(value_required_val);
		}

		return string();
	}

public:
public:
	unique_ptr<Type> key;
	int64_t key_id;
	string type;
	unique_ptr<Type> value;
	int64_t value_id;
	bool value_required;
};

} // namespace rest_api_objects
} // namespace duckdb
