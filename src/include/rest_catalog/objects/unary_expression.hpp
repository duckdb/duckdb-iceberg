
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression_type.hpp"
#include "rest_catalog/objects/term.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class UnaryExpression {
public:
	UnaryExpression();
	UnaryExpression(const UnaryExpression &) = delete;
	UnaryExpression &operator=(const UnaryExpression &) = delete;
	UnaryExpression(UnaryExpression &&) = default;
	UnaryExpression &operator=(UnaryExpression &&) = default;

public:
	static UnaryExpression FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	ExpressionType type;
	Term term;
	yyjson_val *value;
};

} // namespace rest_api_objects
} // namespace duckdb
