
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/and_or_expression.hpp"
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
	Expression() {
	}

public:
	static Expression FromJSON(yyjson_val *obj) {
		Expression res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		do {
			error = true_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_true_expression = true;
				break;
			}
			error = false_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_false_expression = true;
				break;
			}
			error = and_or_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_and_or_expression = true;
				break;
			}
			error = not_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_not_expression = true;
				break;
			}
			error = set_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_set_expression = true;
				break;
			}
			error = literal_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_literal_expression = true;
				break;
			}
			error = unary_expression.TryFromJSON(obj);
			if (error.empty()) {
				has_unary_expression = true;
				break;
			}
			return "Expression failed to parse, none of the oneOf candidates matched";
		} while (false);

		return string();
	}

public:
	SetExpression set_expression;
	FalseExpression false_expression;
	LiteralExpression literal_expression;
	AndOrExpression and_or_expression;
	TrueExpression true_expression;
	UnaryExpression unary_expression;
	NotExpression not_expression;

public:
	bool has_true_expression = false;
	bool has_false_expression = false;
	bool has_and_or_expression = false;
	bool has_not_expression = false;
	bool has_set_expression = false;
	bool has_literal_expression = false;
	bool has_unary_expression = false;
};

} // namespace rest_api_objects
} // namespace duckdb
