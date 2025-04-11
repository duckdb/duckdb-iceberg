
#include "rest_catalog/objects/list_type.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ListType::ListType() {
}

ListType ListType::FromJSON(yyjson_val *obj) {
	ListType res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ListType::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "ListType required property 'type' is missing";
	} else {
		type = yyjson_get_str(type_val);
	}
	auto element_id_val = yyjson_obj_get(obj, "element-id");
	if (!element_id_val) {
		return "ListType required property 'element-id' is missing";
	} else {
		element_id = yyjson_get_sint(element_id_val);
	}
	auto element_val = yyjson_obj_get(obj, "element");
	if (!element_val) {
		return "ListType required property 'element' is missing";
	} else {
		element = make_uniq<Type>();
		error = element->TryFromJSON(element_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto element_required_val = yyjson_obj_get(obj, "element-required");
	if (!element_required_val) {
		return "ListType required property 'element-required' is missing";
	} else {
		element_required = yyjson_get_bool(element_required_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
