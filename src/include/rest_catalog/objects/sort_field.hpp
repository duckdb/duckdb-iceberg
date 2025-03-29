#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/null_order.hpp"
#include "rest_catalog/objects/sort_direction.hpp"
#include "rest_catalog/objects/transform.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SortField {
public:
	static SortField FromJSON(yyjson_val *obj) {
		SortField result;

		auto direction_val = yyjson_obj_get(obj, "direction");
		if (direction_val) {
			result.direction = SortDirection::FromJSON(direction_val);
		} else {
			throw IOException("SortField required property 'direction' is missing");
		}

		auto null_order_val = yyjson_obj_get(obj, "null-order");
		if (null_order_val) {
			result.null_order = NullOrder::FromJSON(null_order_val);
		} else {
			throw IOException("SortField required property 'null-order' is missing");
		}

		auto source_id_val = yyjson_obj_get(obj, "source-id");
		if (source_id_val) {
			result.source_id = yyjson_get_sint(source_id_val);
		} else {
			throw IOException("SortField required property 'source-id' is missing");
		}

		auto transform_val = yyjson_obj_get(obj, "transform");
		if (transform_val) {
			result.transform = Transform::FromJSON(transform_val);
		} else {
			throw IOException("SortField required property 'transform' is missing");
		}

		return result;
	}

public:
	SortDirection direction;
	NullOrder null_order;
	int64_t source_id;
	Transform transform;
};
} // namespace rest_api_objects
} // namespace duckdb
