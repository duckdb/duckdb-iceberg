
#include "rest_catalog/objects/multi_valued_map.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

MultiValuedMap::MultiValuedMap() {
}

MultiValuedMap MultiValuedMap::FromJSON(yyjson_val *obj) {
	MultiValuedMap res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

MultiValuedMap MultiValuedMap::Copy() const {
	MultiValuedMap res;
	for (auto &entry : additional_properties) {
		res.additional_properties.emplace(entry.first, entry.second);
	}
	return res;
}

string MultiValuedMap::TryFromJSON(yyjson_val *obj) {
	string error;
	size_t idx, max;
	yyjson_val *key, *val;
	yyjson_obj_foreach(obj, idx, max, key, val) {
		auto key_str = yyjson_get_str(key);
		vector<string> tmp;
		if (yyjson_is_arr(val)) {
			size_t tmp_idx, tmp_max;
			yyjson_val *tmp_item_val;
			yyjson_arr_foreach(val, tmp_idx, tmp_max, tmp_item_val) {
				string tmp_item;
				if (yyjson_is_str(tmp_item_val)) {
					tmp_item = yyjson_get_str(tmp_item_val);
				} else {
					return StringUtil::Format(
					    "MultiValuedMap property 'tmp_item' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(tmp_item_val));
				}
				tmp.emplace_back(std::move(tmp_item));
			}
		} else {
			return StringUtil::Format("MultiValuedMap property 'tmp' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(val));
		}
		additional_properties.emplace(key_str, std::move(tmp));
	}
	return "";
}

void MultiValuedMap::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize additional properties
	for (const auto &it : additional_properties) {
		auto &key = it.first;
		auto &value = it.second;
		yyjson_mut_val *value_obj = yyjson_mut_arr(doc);
		for (const auto &array_item : value) {
			yyjson_mut_arr_append(value_obj, yyjson_mut_strcpy(doc, array_item.c_str()));
		}
		auto key_ptr = unsafe_yyjson_mut_strncpy(doc, key.c_str(), strlen(key.c_str()));
		yyjson_mut_obj_add_val(doc, obj, key_ptr, value_obj);
	}
}

yyjson_mut_val *MultiValuedMap::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
