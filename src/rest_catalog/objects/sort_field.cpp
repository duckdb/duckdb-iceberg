
#include "rest_catalog/objects/sort_field.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SortField::SortField() {
}

SortField SortField::FromJSON(yyjson_val *obj) {
	SortField res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SortField::TryFromJSON(yyjson_val *obj) {
	string error;
	auto source_id_val = yyjson_obj_get(obj, "source-id");
	if (!source_id_val) {
		return "SortField required property 'source-id' is missing";
	} else {
		if (yyjson_is_int(source_id_val)) {
			source_id = yyjson_get_int(source_id_val);
		} else {
			return StringUtil::Format("SortField property 'source_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(source_id_val));
		}
	}
	auto transform_val = yyjson_obj_get(obj, "transform");
	if (!transform_val) {
		return "SortField required property 'transform' is missing";
	} else {
		error = transform.TryFromJSON(transform_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto direction_val = yyjson_obj_get(obj, "direction");
	if (!direction_val) {
		return "SortField required property 'direction' is missing";
	} else {
		error = direction.TryFromJSON(direction_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto null_order_val = yyjson_obj_get(obj, "null-order");
	if (!null_order_val) {
		return "SortField required property 'null-order' is missing";
	} else {
		error = null_order.TryFromJSON(null_order_val);
		if (!error.empty()) {
			return error;
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
