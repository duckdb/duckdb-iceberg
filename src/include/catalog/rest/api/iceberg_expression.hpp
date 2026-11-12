
#pragma once

#include "duckdb/planner/expression.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

class IcebergExpression {
public:
	static rest_api_objects::Term ReferenceExpression(const string &column_name);
	static unique_ptr<rest_api_objects::Predicate> LiteralExpression(const string &type, const string &column_name,
	                                                                 const Value &value);
	static unique_ptr<rest_api_objects::Predicate> UnaryExpression(const string &type, const string &column_name);
	static unique_ptr<rest_api_objects::Predicate> SetExpression(const string &type, const string &column_name,
	                                                             const vector<reference<const Value>> &values);
	static unique_ptr<rest_api_objects::Predicate> AndExpression(unique_ptr<rest_api_objects::Predicate> left,
	                                                             unique_ptr<rest_api_objects::Predicate> right);
	static optional<string> GetComparisonType(ExpressionType type, bool flip);
	static unique_ptr<rest_api_objects::Predicate> TryConvertFilter(const Expression &expr, const string &column_name);
};

} // namespace duckdb
