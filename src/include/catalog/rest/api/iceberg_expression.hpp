
#pragma once

#include "duckdb/planner/expression.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

class IcebergExpression {
public:
	static rest_api_objects::Term ReferenceExpression(const string &column_name);
	static unique_ptr<rest_api_objects::Expression> LiteralExpression(const string &type, const string &column_name,
	                                                                  const Value &value);
	static unique_ptr<rest_api_objects::Expression> UnaryExpression(const string &type, const string &column_name);
	static unique_ptr<rest_api_objects::Expression> AndExpression(unique_ptr<rest_api_objects::Expression> left,
	                                                              unique_ptr<rest_api_objects::Expression> right);
	static optional<string> GetComparisonType(ExpressionType type, bool flip);
	static unique_ptr<rest_api_objects::Expression> TryConvertFilter(const Expression &expr, const string &column_name);
};

} // namespace duckdb
