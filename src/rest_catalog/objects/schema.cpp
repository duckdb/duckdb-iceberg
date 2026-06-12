
#include "rest_catalog/objects/schema.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Schema::Schema() {
}
Schema::Object1::Object1() {
}

Schema::Object1 Schema::Object1::FromJSON(yyjson_val *obj) {
	Object1 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Schema::Object1 Schema::Object1::Copy() const {
	Object1 res;
	if (has_schema_id) {
		res.schema_id = schema_id;
	}
	res.has_schema_id = has_schema_id;
	if (has_identifier_field_ids) {
		res.identifier_field_ids.reserve(identifier_field_ids.size());
		for (auto &item : identifier_field_ids) {
			res.identifier_field_ids.emplace_back(item);
		}
	}
	res.has_identifier_field_ids = has_identifier_field_ids;
	return res;
}
string Schema::Object1::TryFromJSON(yyjson_val *obj) {
	string error;
	auto schema_id_val = yyjson_obj_get(obj, "schema-id");
	if (schema_id_val && !yyjson_is_null(schema_id_val)) {
		has_schema_id = true;
		if (yyjson_is_int(schema_id_val)) {
			schema_id = yyjson_get_int(schema_id_val);
		} else {
			return StringUtil::Format("Object1 property 'schema_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(schema_id_val));
		}
	}
	auto identifier_field_ids_val = yyjson_obj_get(obj, "identifier-field-ids");
	if (identifier_field_ids_val && !yyjson_is_null(identifier_field_ids_val)) {
		has_identifier_field_ids = true;
		if (yyjson_is_arr(identifier_field_ids_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(identifier_field_ids_val, idx, max, val) {
				int32_t tmp;
				if (yyjson_is_int(val)) {
					tmp = yyjson_get_int(val);
				} else {
					return StringUtil::Format("Object1 property 'tmp' is not of type 'integer', found '%s' instead",
					                          yyjson_get_type_desc(val));
				}
				identifier_field_ids.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "Object1 property 'identifier_field_ids' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(identifier_field_ids_val));
		}
	}
	return string();
}

Schema Schema::FromJSON(yyjson_val *obj) {
	Schema res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Schema Schema::Copy() const {
	Schema res;
	res.struct_type = struct_type.Copy();
	res.object_1 = object_1.Copy();
	return res;
}
string Schema::TryFromJSON(yyjson_val *obj) {
	string error;
	error = struct_type.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = object_1.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
