
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

class SetExpression {
public:
	SetExpression();
	SetExpression(const SetExpression &) = delete;
	SetExpression &operator=(const SetExpression &) = delete;
	SetExpression(SetExpression &&) = default;
	SetExpression &operator=(SetExpression &&) = default;

public:
	static SetExpression FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	ExpressionType type;
	Term term;
	vector<yyjson_val *> values;
};

} // namespace rest_api_objects
} // namespace duckdb
