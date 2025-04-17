
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

class TrueExpression {
public:
	TrueExpression();
	TrueExpression(const TrueExpression &) = delete;
	TrueExpression &operator=(const TrueExpression &) = delete;
	TrueExpression(TrueExpression &&) = default;
	TrueExpression &operator=(TrueExpression &&) = default;

public:
	static TrueExpression FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	ExpressionType type;
};

} // namespace rest_api_objects
} // namespace duckdb
