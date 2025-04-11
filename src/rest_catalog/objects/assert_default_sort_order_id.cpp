
#include "rest_catalog/objects/assert_default_sort_order_id.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssertDefaultSortOrderId::AssertDefaultSortOrderId() {
}

AssertDefaultSortOrderId AssertDefaultSortOrderId::FromJSON(yyjson_val *obj) {
	AssertDefaultSortOrderId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string AssertDefaultSortOrderId::TryFromJSON(yyjson_val *obj) {
	string error;
	error = table_requirement.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto default_sort_order_id_val = yyjson_obj_get(obj, "default-sort-order-id");
	if (!default_sort_order_id_val) {
		return "AssertDefaultSortOrderId required property 'default-sort-order-id' is missing";
	} else {
		default_sort_order_id = yyjson_get_sint(default_sort_order_id_val);
	}
	auto type_val = yyjson_obj_get(obj, "type");
	if (type_val) {
		has_type = true;
		type = yyjson_get_str(type_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
