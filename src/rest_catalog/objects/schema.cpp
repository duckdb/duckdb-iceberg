
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
	if (schema_id.has_value()) {
		res.schema_id.emplace();
		(*res.schema_id) = (*schema_id);
	}
	if (identifier_field_ids.has_value()) {
		res.identifier_field_ids.emplace();
		(*res.identifier_field_ids).reserve((*identifier_field_ids).size());
		for (auto &item : (*identifier_field_ids)) {
			(*res.identifier_field_ids).emplace_back(item);
		}
	}
	return res;
}

string Schema::Object1::TryFromJSON(yyjson_val *obj) {
	string error;
	auto schema_id_val = yyjson_obj_get(obj, "schema-id");
	if (schema_id_val) {
		int32_t schema_id_tmp;
		if (yyjson_is_int(schema_id_val)) {
			schema_id_tmp = yyjson_get_int(schema_id_val);
		} else {
			return StringUtil::Format("Object1 property 'schema_id_tmp' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(schema_id_val));
		}
		schema_id = std::move(schema_id_tmp);
	}
	auto identifier_field_ids_val = yyjson_obj_get(obj, "identifier-field-ids");
	if (identifier_field_ids_val) {
		vector<int32_t> identifier_field_ids_tmp;
		if (yyjson_is_arr(identifier_field_ids_val)) {
			size_t identifier_field_ids_tmp_idx, identifier_field_ids_tmp_max;
			yyjson_val *identifier_field_ids_tmp_item_val;
			yyjson_arr_foreach(identifier_field_ids_val, identifier_field_ids_tmp_idx, identifier_field_ids_tmp_max,
			                   identifier_field_ids_tmp_item_val) {
				int32_t identifier_field_ids_tmp_item;
				if (yyjson_is_int(identifier_field_ids_tmp_item_val)) {
					identifier_field_ids_tmp_item = yyjson_get_int(identifier_field_ids_tmp_item_val);
				} else {
					return StringUtil::Format(
					    "Object1 property 'identifier_field_ids_tmp_item' is not of type 'integer', found '%s' instead",
					    yyjson_get_type_desc(identifier_field_ids_tmp_item_val));
				}
				identifier_field_ids_tmp.emplace_back(std::move(identifier_field_ids_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "Object1 property 'identifier_field_ids_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(identifier_field_ids_val));
		}
		identifier_field_ids = std::move(identifier_field_ids_tmp);
	}
	return "";
}

void Schema::Object1::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: schema-id
	if (schema_id.has_value()) {
		auto &schema_id_value = *schema_id;
		yyjson_mut_obj_add_int(doc, obj, "schema-id", schema_id_value);
	}

	// Serialize: identifier-field-ids
	if (identifier_field_ids.has_value()) {
		auto &identifier_field_ids_value = *identifier_field_ids;
		yyjson_mut_val *identifier_field_ids_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : identifier_field_ids_value) {
			yyjson_mut_val *item_val = yyjson_mut_int(doc, item);
			yyjson_mut_arr_append(identifier_field_ids_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "identifier-field-ids", identifier_field_ids_value_arr);
	}
}

yyjson_mut_val *Schema::Object1::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
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
	return "";
}

void Schema::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: StructType
	struct_type.PopulateJSON(doc, obj);

	// Serialize base class: Object1
	object_1.PopulateJSON(doc, obj);
}

yyjson_mut_val *Schema::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
