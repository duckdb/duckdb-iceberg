
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

class LiteralExpression {
public:
	LiteralExpression();
	LiteralExpression(const LiteralExpression &) = delete;
	LiteralExpression &operator=(const LiteralExpression &) = delete;
	LiteralExpression(LiteralExpression &&) = default;
	LiteralExpression &operator=(LiteralExpression &&) = default;

public:
	static LiteralExpression FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	ExpressionType type;
	Term term;
	yyjson_val *value;
};

} // namespace rest_api_objects
} // namespace duckdb
