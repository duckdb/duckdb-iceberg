
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

class NotExpression {
public:
	NotExpression();
	NotExpression(const NotExpression &) = delete;
	NotExpression &operator=(const NotExpression &) = delete;
	NotExpression(NotExpression &&) = default;
	NotExpression &operator=(NotExpression &&) = default;

public:
	static NotExpression FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	ExpressionType type;
	unique_ptr<Expression> child;
};

} // namespace rest_api_objects
} // namespace duckdb
