
#include "rest_catalog/objects/value_map.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ValueMap::ValueMap() {
}

ValueMap ValueMap::FromJSON(yyjson_val *obj) {
	ValueMap res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ValueMap::TryFromJSON(yyjson_val *obj) {
	string error;
	auto keys_val = yyjson_obj_get(obj, "keys");
	if (keys_val) {
		has_keys = true;
		if (yyjson_is_arr(keys_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(keys_val, idx, max, val) {
				IntegerTypeValue tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				keys.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("ValueMap property 'keys' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(keys_val));
		}
	}
	auto values_val = yyjson_obj_get(obj, "values");
	if (values_val) {
		has_values = true;
		if (yyjson_is_arr(values_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(values_val, idx, max, val) {
				PrimitiveTypeValue tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				values.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("ValueMap property 'values' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(values_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
