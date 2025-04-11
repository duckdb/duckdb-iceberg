
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Expression;

class AndOrExpression {
public:
	AndOrExpression();
	AndOrExpression(const AndOrExpression &) = delete;
	AndOrExpression &operator=(const AndOrExpression &) = delete;
	AndOrExpression(AndOrExpression &&) = default;
	AndOrExpression &operator=(AndOrExpression &&) = default;

public:
	static AndOrExpression FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	ExpressionType type;
	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
};

} // namespace rest_api_objects
} // namespace duckdb
