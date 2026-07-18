
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/and_or_expression.hpp"
#include "rest_catalog/objects/boolean_expression.hpp"
#include "rest_catalog/objects/false_expression.hpp"
#include "rest_catalog/objects/literal_expression.hpp"
#include "rest_catalog/objects/not_expression.hpp"
#include "rest_catalog/objects/set_expression.hpp"
#include "rest_catalog/objects/true_expression.hpp"
#include "rest_catalog/objects/unary_expression.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Expression {
public:
	Expression();
	Expression(const Expression &) = delete;
	Expression &operator=(const Expression &) = delete;
	Expression(Expression &&) = default;
	Expression &operator=(Expression &&) = default;

public:
	// Deserialization
	static Expression FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	Expression Copy() const;

	// Serialization
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	optional<BooleanExpression> boolean_expression;
	optional<TrueExpression> true_expression;
	optional<FalseExpression> false_expression;
	optional<AndOrExpression> and_or_expression;
	optional<NotExpression> not_expression;
	optional<SetExpression> set_expression;
	optional<LiteralExpression> literal_expression;
	optional<UnaryExpression> unary_expression;
};

} // namespace rest_api_objects
} // namespace duckdb
