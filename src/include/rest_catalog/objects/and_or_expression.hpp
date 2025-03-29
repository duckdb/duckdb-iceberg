#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression.hpp"
#include "rest_catalog/objects/expression_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AndOrExpression {
public:
	static AndOrExpression FromJSON(yyjson_val *obj) {
		AndOrExpression result;

		auto left_val = yyjson_obj_get(obj, "left");
		if (left_val) {
			result.left = Expression::FromJSON(left_val);
		} else {
			throw IOException("AndOrExpression required property 'left' is missing");
		}

		auto right_val = yyjson_obj_get(obj, "right");
		if (right_val) {
			result.right = Expression::FromJSON(right_val);
		} else {
			throw IOException("AndOrExpression required property 'right' is missing");
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		} else {
			throw IOException("AndOrExpression required property 'type' is missing");
		}

		return result;
	}

public:
	Expression left;
	Expression right;
	ExpressionType type;
};
} // namespace rest_api_objects
} // namespace duckdb
