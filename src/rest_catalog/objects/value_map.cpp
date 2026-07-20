
#include "rest_catalog/objects/value_map.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

ValueMap ValueMap::Copy() const {
	ValueMap res;
	if (keys.has_value()) {
		res.keys.emplace();
		(*res.keys).reserve((*keys).size());
		for (auto &item : (*keys)) {
			(*res.keys).emplace_back(item.Copy());
		}
	}
	if (values.has_value()) {
		res.values.emplace();
		(*res.values).reserve((*values).size());
		for (auto &item : (*values)) {
			(*res.values).emplace_back(item.Copy());
		}
	}
	return res;
}

string ValueMap::TryFromJSON(yyjson_val *obj) {
	string error;
	auto keys_val = yyjson_obj_get(obj, "keys");
	if (keys_val) {
		vector<IntegerTypeValue> keys_tmp;
		if (yyjson_is_arr(keys_val)) {
			size_t keys_tmp_idx, keys_tmp_max;
			yyjson_val *keys_tmp_item_val;
			yyjson_arr_foreach(keys_val, keys_tmp_idx, keys_tmp_max, keys_tmp_item_val) {
				IntegerTypeValue keys_tmp_item;
				error = keys_tmp_item.TryFromJSON(keys_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				keys_tmp.emplace_back(std::move(keys_tmp_item));
			}
		} else {
			return StringUtil::Format("ValueMap property 'keys_tmp' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(keys_val));
		}
		keys = std::move(keys_tmp);
	}
	auto values_val = yyjson_obj_get(obj, "values");
	if (values_val) {
		vector<PrimitiveTypeValue> values_tmp;
		if (yyjson_is_arr(values_val)) {
			size_t values_tmp_idx, values_tmp_max;
			yyjson_val *values_tmp_item_val;
			yyjson_arr_foreach(values_val, values_tmp_idx, values_tmp_max, values_tmp_item_val) {
				PrimitiveTypeValue values_tmp_item;
				error = values_tmp_item.TryFromJSON(values_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				values_tmp.emplace_back(std::move(values_tmp_item));
			}
		} else {
			return StringUtil::Format("ValueMap property 'values_tmp' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(values_val));
		}
		values = std::move(values_tmp);
	}
	return "";
}

void ValueMap::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: keys
	if (keys.has_value()) {
		auto &keys_value = *keys;
		yyjson_mut_val *keys_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : keys_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(keys_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "keys", keys_value_arr);
	}

	// Serialize: values
	if (values.has_value()) {
		auto &values_value = *values;
		yyjson_mut_val *values_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : values_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(values_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "values", values_value_arr);
	}
}

yyjson_mut_val *ValueMap::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
