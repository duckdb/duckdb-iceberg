#include "iceberg_predicate.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

namespace {

struct BoundExpressionReplacer : public LogicalOperatorVisitor {
public:
	BoundExpressionReplacer(const Value &val) : val(val) {
	}

public:
	unique_ptr<Expression> VisitReplace(BoundReferenceExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (expr.index != 0) {
			return nullptr;
		}
		return make_uniq<BoundConstantExpression>(val);
	}

public:
	const Value &val;
};

} // namespace

template <class TRANSFORM>
bool MatchBoundsTemplated(ClientContext &context, const TableFilter &filter, const IcebergPredicateStats &stats,
                          const IcebergTransform &transform);

template <class TRANSFORM>
static bool MatchBoundsConstant(const Value &constant, ExpressionType comparison_type,
                                const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	auto constant_value = TRANSFORM::ApplyTransform(constant, transform);
	if (constant_value.IsNull() || stats.lower_bound.IsNull() || stats.upper_bound.IsNull()) {
		//! Can't compare when there are no bounds
		return true;
	}

	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return TRANSFORM::CompareEqual(constant_value, stats);
	case ExpressionType::COMPARE_GREATERTHAN:
		return TRANSFORM::CompareGreaterThan(constant_value, stats);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return TRANSFORM::CompareGreaterThanOrEqual(constant_value, stats);
	case ExpressionType::COMPARE_LESSTHAN:
		return TRANSFORM::CompareLessThan(constant_value, stats);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return TRANSFORM::CompareLessThanOrEqual(constant_value, stats);
	default:
		//! Conservative approach: we don't know, so we just say it's not filtered out
		return true;
	}
}

template <class TRANSFORM>
static bool MatchBoundsConstantFilter(const ConstantFilter &constant_filter, const IcebergPredicateStats &stats,
                                      const IcebergTransform &transform) {
	return MatchBoundsConstant<TRANSFORM>(constant_filter.constant, constant_filter.comparison_type, stats, transform);
}

template <class TRANSFORM>
static bool MatchBoundsIsNullFilter(const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	return stats.has_null == true;
}

template <class TRANSFORM>
static bool MatchBoundsIsNotNullFilter(const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	return stats.has_not_null == true;
}

template <class TRANSFORM>
static bool MatchBoundsConjunctionAndFilter(ClientContext &context, const ConjunctionAndFilter &conjunction_and,
                                            const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	for (auto &child : conjunction_and.child_filters) {
		if (!MatchBoundsTemplated<TRANSFORM>(context, *child, stats, transform)) {
			return false;
		}
	}
	return true;
}

template <class TRANSFORM>
bool MatchTransformedBounds(ClientContext &context, ExpressionType comparison_type, const Expression &left,
                            const Expression &right, const IcebergPredicateStats &stats,
                            const IcebergTransform &transform) {
	BoundExpressionReplacer lower_replacer(stats.lower_bound);
	BoundExpressionReplacer upper_replacer(stats.upper_bound);
	auto lower_copy = left.Copy();
	auto upper_copy = left.Copy();
	lower_replacer.VisitExpression(&lower_copy);
	upper_replacer.VisitExpression(&upper_copy);

	Value right_constant;
	if (!ExpressionExecutor::TryEvaluateScalar(context, right, right_constant)) {
		return true;
	}

	Value transformed_lower_bound;
	Value transformed_upper_bound;
	if (!ExpressionExecutor::TryEvaluateScalar(context, *lower_copy, transformed_lower_bound)) {
		return true;
	}
	if (!ExpressionExecutor::TryEvaluateScalar(context, *upper_copy, transformed_upper_bound)) {
		return true;
	}
	IcebergPredicateStats transformed_stats(stats);
	transformed_stats.lower_bound = transformed_lower_bound;
	transformed_stats.upper_bound = transformed_upper_bound;

	return MatchBoundsConstant<TRANSFORM>(right_constant, comparison_type, transformed_stats, transform);
}

template <class TRANSFORM>
bool MatchBoundsTemplated(ClientContext &context, const TableFilter &filter, const IcebergPredicateStats &stats,
                          const IcebergTransform &transform) {
	//! TODO: support more filter types
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		return MatchBoundsConstantFilter<TRANSFORM>(constant_filter, stats, transform);
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and_filter = filter.Cast<ConjunctionAndFilter>();
		return MatchBoundsConjunctionAndFilter<TRANSFORM>(context, conjunction_and_filter, stats, transform);
	}
	case TableFilterType::IS_NULL: {
		//! FIXME: these are never hit, because it goes through ExpressionFilter instead?
		return MatchBoundsIsNullFilter<TRANSFORM>(stats, transform);
	}
	case TableFilterType::IS_NOT_NULL: {
		//! FIXME: these are never hit, because it goes through ExpressionFilter instead?
		return MatchBoundsIsNotNullFilter<TRANSFORM>(stats, transform);
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (optional_filter.child_filter) {
			return MatchBoundsTemplated<TRANSFORM>(context, *optional_filter.child_filter, stats, transform);
		}
		//! child filter wasn't populated (yet?) for some reason, just be conservative
		return true;
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		D_ASSERT(!in_filter.values.empty());
		for (auto &value : in_filter.values) {
			if (MatchBoundsConstant<TRANSFORM>(value, ExpressionType::COMPARE_EQUAL, stats, transform)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::EXPRESSION_FILTER: {
		//! Expressions can be arbitrarily complex, and we currently only support IS NULL/IS NOT NULL checks against the
		//! column itself, i.e. where the expression is a BOUND_OPERATOR with type OPERATOR_IS_NULL/_IS_NOT_NULL with a
		//! single child expression of type BOUND_REF.
		//!
		//! See duckdb/duckdb-iceberg#464
		auto &expression_filter = filter.Cast<ExpressionFilter>();
		auto &expr = *expression_filter.expr;

		switch (expr.type) {
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			D_ASSERT(expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR);
			auto &bound_operator_expr = expr.Cast<BoundOperatorExpression>();

			D_ASSERT(bound_operator_expr.children.size() == 1);
			auto &child_expr = bound_operator_expr.children[0];
			if (child_expr->type != ExpressionType::BOUND_REF) {
				//! We can't evaluate expressions that aren't direct column references
				return true;
			}

			if (expr.type == ExpressionType::OPERATOR_IS_NULL) {
				return MatchBoundsIsNullFilter<TRANSFORM>(stats, transform);
			}
			D_ASSERT(expr.type == ExpressionType::OPERATOR_IS_NOT_NULL);
			return MatchBoundsIsNotNullFilter<TRANSFORM>(stats, transform);
		}
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_EQUAL: {
			D_ASSERT(expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
			auto &compare_expr = expr.Cast<BoundComparisonExpression>();
			if (transform.Type() == IcebergTransformType::IDENTITY) {
				//! No further processing has been done on the stats (lower/upper bounds)
				auto &left = *compare_expr.left;
				auto &right = *compare_expr.right;

				bool left_foldable = left.IsFoldable();
				bool right_foldable = right.IsFoldable();
				if (!left_foldable && !right_foldable) {
					//! Both are not foldable, can't evaluate at all
					return true;
				}

				if (left_foldable) {
					return MatchTransformedBounds<TRANSFORM>(context, expr.type, right, left, stats, transform);
				} else {
					return MatchTransformedBounds<TRANSFORM>(context, expr.type, left, right, stats, transform);
				}
				return true;
			}
		}
		default:
			return true;
		}
	}
	default:
		//! Conservative approach: we don't know what this is, just say it doesn't filter anything
		return true;
	}
}

bool IcebergPredicate::MatchBounds(ClientContext &context, const TableFilter &filter,
                                   const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	switch (transform.Type()) {
	case IcebergTransformType::IDENTITY:
		return MatchBoundsTemplated<IdentityTransform>(context, filter, stats, transform);
	case IcebergTransformType::BUCKET:
		return true;
	case IcebergTransformType::TRUNCATE:
		return true;
	case IcebergTransformType::YEAR:
		return MatchBoundsTemplated<YearTransform>(context, filter, stats, transform);
	case IcebergTransformType::MONTH:
		return MatchBoundsTemplated<MonthTransform>(context, filter, stats, transform);
	case IcebergTransformType::DAY:
		return MatchBoundsTemplated<DayTransform>(context, filter, stats, transform);
	case IcebergTransformType::HOUR:
		return MatchBoundsTemplated<HourTransform>(context, filter, stats, transform);
	case IcebergTransformType::VOID:
		return true;
	default:
		throw InvalidConfigurationException("Transform '%s' not implemented", transform.RawType());
	}
}

} // namespace duckdb
