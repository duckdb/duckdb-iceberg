
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
	AndOrExpression::AndOrExpression() {
	}

public:
	static AndOrExpression FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
		return "AndOrExpression required property 'type' is missing");
		}
		error = expression_type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}

		auto left_val = yyjson_obj_get(obj, "left");
		if (!left_val) {
		return "AndOrExpression required property 'left' is missing");
		}
		error = expression.TryFromJSON(left_val);
		if (!error.empty()) {
			return error;
		}

		auto right_val = yyjson_obj_get(obj, "right");
		if (!right_val) {
		return "AndOrExpression required property 'right' is missing");
		}
		error = expression.TryFromJSON(right_val);
		if (!error.empty()) {
			return error;
		}

		return string();
	}

public:
public:
	Expression left;
	Expression right;
	ExpressionType type;
};

} // namespace rest_api_objects
} // namespace duckdb
