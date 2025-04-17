
#include "rest_catalog/objects/struct_type.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
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
	}
	auto fields_val = yyjson_obj_get(obj, "fields");
	if (!fields_val) {
		return "StructType required property 'fields' is missing";
	} else {
		if (yyjson_is_arr(fields_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(fields_val, idx, max, val) {
				auto tmp_p = make_uniq<StructField>();
				auto &tmp = *tmp_p;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				fields.emplace_back(std::move(tmp_p));
			}
		} else {
			return StringUtil::Format("StructType property 'fields' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(fields_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
