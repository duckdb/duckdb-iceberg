
#include "rest_catalog/objects/set_default_sort_order_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SetDefaultSortOrderUpdate::SetDefaultSortOrderUpdate() {
}

SetDefaultSortOrderUpdate SetDefaultSortOrderUpdate::FromJSON(yyjson_val *obj) {
	SetDefaultSortOrderUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SetDefaultSortOrderUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto sort_order_id_val = yyjson_obj_get(obj, "sort_order_id");
	if (!sort_order_id_val) {
		return "SetDefaultSortOrderUpdate required property 'sort_order_id' is missing";
	} else {
		sort_order_id = yyjson_get_sint(sort_order_id_val);
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		action = yyjson_get_str(action_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
