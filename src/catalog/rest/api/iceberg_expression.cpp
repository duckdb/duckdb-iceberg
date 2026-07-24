#include "catalog/rest/api/iceberg_expression.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "catalog/rest/api/iceberg_type.hpp"

namespace duckdb {

rest_api_objects::Term IcebergExpression::ReferenceExpression(const string &column_name) {
	rest_api_objects::Term result;
	result.reference.emplace();
	result.reference->value = column_name;
	return result;
}

unique_ptr<rest_api_objects::Expression>
IcebergExpression::LiteralExpression(const string &type, const string &column_name, const Value &value) {
	if (value.IsNull()) {
		return nullptr;
	}
	auto result = make_uniq<rest_api_objects::Expression>();
	result->literal_expression.emplace();
	result->literal_expression->type.value = type;
	result->literal_expression->term = ReferenceExpression(column_name);
	try {
		result->literal_expression->value = IcebergTypeHelper::PrimitiveTypeFromValue(value);
	} catch (...) {
		return nullptr;
	}
	return result;
}

unique_ptr<rest_api_objects::Expression> IcebergExpression::UnaryExpression(const string &type,
                                                                            const string &column_name) {
	auto result = make_uniq<rest_api_objects::Expression>();
	result->unary_expression.emplace();
	result->unary_expression->type.value = type;
	result->unary_expression->term = ReferenceExpression(column_name);
	return result;
}

unique_ptr<rest_api_objects::Expression>
IcebergExpression::AndExpression(unique_ptr<rest_api_objects::Expression> left,
                                 unique_ptr<rest_api_objects::Expression> right) {
	if (!left) {
		return right;
	}
	if (!right) {
		return left;
	}
	auto result = make_uniq<rest_api_objects::Expression>();
	result->and_or_expression.emplace();
	result->and_or_expression->type.value = "and";
	result->and_or_expression->left = std::move(left);
	result->and_or_expression->right = std::move(right);
	return result;
}

optional<string> IcebergExpression::GetComparisonType(ExpressionType type, bool flip) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "eq";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "not-eq";
	case ExpressionType::COMPARE_LESSTHAN:
		return flip ? "gt" : "lt";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return flip ? "gt-eq" : "lt-eq";
	case ExpressionType::COMPARE_GREATERTHAN:
		return flip ? "lt" : "gt";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return flip ? "lt-eq" : "gt-eq";
	default:
		return nullopt;
	}
}

unique_ptr<rest_api_objects::Expression> IcebergExpression::TryConvertFilter(const Expression &expr,
                                                                             const string &column_name) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		const bool is_and = expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND;
		unique_ptr<rest_api_objects::Expression> result;
		for (auto &child : conjunction.GetChildren()) {
			auto converted = TryConvertFilter(*child, column_name);
			if (!converted) {
				// Dropping an unsupported conjunct is safe; dropping an OR branch is not.
				if (!is_and) {
					return nullptr;
				}
				continue;
			}
			if (!result) {
				result = std::move(converted);
				continue;
			}
			auto combined = make_uniq<rest_api_objects::Expression>();
			combined->and_or_expression.emplace();
			combined->and_or_expression->type.value = is_and ? "and" : "or";
			combined->and_or_expression->left = std::move(result);
			combined->and_or_expression->right = std::move(converted);
			result = std::move(combined);
		}
		return result;
	}
	if (BoundComparisonExpression::IsComparison(expr)) {
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		bool flip = false;
		optional_ptr<const BoundConstantExpression> constant;
		if (left.GetExpressionClass() == ExpressionClass::BOUND_REF &&
		    right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			constant = &right.Cast<BoundConstantExpression>();
		} else if (right.GetExpressionClass() == ExpressionClass::BOUND_REF &&
		           left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			constant = &left.Cast<BoundConstantExpression>();
			flip = true;
		} else {
			return nullptr;
		}
		auto type = GetComparisonType(expr.GetExpressionType(), flip);
		return type ? LiteralExpression(*type, column_name, constant->GetValue()) : nullptr;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto &op = expr.Cast<BoundOperatorExpression>();
		if (op.GetChildren().size() != 1 || op.GetChildren()[0]->GetExpressionClass() != ExpressionClass::BOUND_REF) {
			return nullptr;
		}
		if (expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL) {
			return UnaryExpression("is-null", column_name);
		}
		if (expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
			return UnaryExpression("not-null", column_name);
		}
	}
	return nullptr;
}

} // namespace duckdb
