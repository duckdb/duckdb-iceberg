
#include "rest_catalog/objects/namespace.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Namespace::Namespace() {
}

Namespace Namespace::FromJSON(yyjson_val *obj) {
	Namespace res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Namespace Namespace::Copy() const {
	Namespace res;
	res.value.reserve(value.size());
	for (auto &item : value) {
		res.value.emplace_back(item);
	}
	return res;
}

string Namespace::TryFromJSON(yyjson_val *obj) {
	string error;
	if (yyjson_is_arr(obj)) {
		size_t value_idx, value_max;
		yyjson_val *value_item_val;
		yyjson_arr_foreach(obj, value_idx, value_max, value_item_val) {
			string value_item;
			if (yyjson_is_str(value_item_val)) {
				value_item = yyjson_get_str(value_item_val);
			} else {
				return StringUtil::Format("Namespace property 'value_item' is not of type 'string', found '%s' instead",
				                          yyjson_get_type_desc(value_item_val));
			}
			value.emplace_back(std::move(value_item));
		}
	} else {
		return StringUtil::Format("Namespace property 'value' is not of type 'array', found '%s' instead",
		                          yyjson_get_type_desc(obj));
	}
	return "";
}

yyjson_mut_val *Namespace::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *arr = yyjson_mut_arr(doc);
	for (const auto &item : value) {
		yyjson_mut_arr_append(arr, yyjson_mut_str(doc, item.c_str()));
	}
	return arr;
}

} // namespace rest_api_objects
} // namespace duckdb
