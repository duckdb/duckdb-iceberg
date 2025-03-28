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
	static MapType FromJSON(yyjson_val *obj) {
		MapType result;

		auto key_val = yyjson_obj_get(obj, "key");
		if (key_val) {
			result.key = Type::FromJSON(key_val);
		}
		else {
			throw IOException("MapType required property 'key' is missing");
		}

		auto key_id_val = yyjson_obj_get(obj, "key-id");
		if (key_id_val) {
			result.key_id = yyjson_get_sint(key_id_val);
		}
		else {
			throw IOException("MapType required property 'key-id' is missing");
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		else {
			throw IOException("MapType required property 'type' is missing");
		}

		auto value_val = yyjson_obj_get(obj, "value");
		if (value_val) {
			result.value = Type::FromJSON(value_val);
		}
		else {
			throw IOException("MapType required property 'value' is missing");
		}

		auto value_id_val = yyjson_obj_get(obj, "value-id");
		if (value_id_val) {
			result.value_id = yyjson_get_sint(value_id_val);
		}
		else {
			throw IOException("MapType required property 'value-id' is missing");
		}

		auto value_required_val = yyjson_obj_get(obj, "value-required");
		if (value_required_val) {
			result.value_required = yyjson_get_bool(value_required_val);
		}
		else {
			throw IOException("MapType required property 'value-required' is missing");
		}

		return result;
	}

public:
	Type key;
	int64_t key_id;
	string type;
	Type value;
	int64_t value_id;
	bool value_required;
};
} // namespace rest_api_objects
} // namespace duckdb