
#include "rest_catalog/objects/struct_type.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

StructType::StructType() {
}

StructType StructType::FromJSON(yyjson_val *obj) {
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

string StructType::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "StructType required property 'type' is missing";
	} else {
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format("StructType property 'type' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(type_val));
		}
		if (!yyjson_is_null(type_val) && type != "struct") {
			return "StructType property 'type' does not match its required const value";
		}
	}
	auto fields_val = yyjson_obj_get(obj, "fields");
	if (!fields_val) {
		return "StructType required property 'fields' is missing";
	} else {
		if (yyjson_is_arr(fields_val)) {
			size_t fields_idx, fields_max;
			yyjson_val *fields_item_val;
			yyjson_arr_foreach(fields_val, fields_idx, fields_max, fields_item_val) {
				auto fields_item_p = make_uniq<StructField>();
				auto &fields_item = *fields_item_p;
				error = fields_item.TryFromJSON(fields_item_val);
				if (!error.empty()) {
					return error;
				}
				fields.emplace_back(std::move(fields_item_p));
			}
		} else {
			return StringUtil::Format("StructType property 'fields' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(fields_val));
		}
	}
	return "";
}

void StructType::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: type
	yyjson_mut_obj_add_strcpy(doc, obj, "type", type.c_str());

	// Serialize: fields
	yyjson_mut_val *fields_arr = yyjson_mut_arr(doc);
	for (const auto &item : fields) {
		yyjson_mut_val *item_val = item->ToJSON(doc);
		yyjson_mut_arr_append(fields_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "fields", fields_arr);
}

yyjson_mut_val *StructType::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
