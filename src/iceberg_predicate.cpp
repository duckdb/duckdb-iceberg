#include "iceberg_predicate.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

template <class TRANSFORM>
bool MatchBoundsTemplated(const TableFilter &filter, const IcebergPredicateStats &stats,
                          const IcebergTransform &transform);

template <class TRANSFORM>
static bool MatchBoundsConstantFilter(const ConstantFilter &constant_filter, const IcebergPredicateStats &stats,
                                      const IcebergTransform &transform) {
	auto constant_value = TRANSFORM::ApplyTransform(constant_filter.constant, transform);

	if (constant_value.IsNull() || stats.lower_bound.IsNull() || stats.upper_bound.IsNull()) {
		//! Can't compare when there are no bounds
		return true;
	}

	switch (constant_filter.comparison_type) {
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
static bool MatchBoundsIsNullFilter(const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	return stats.has_null == true;
}

template <class TRANSFORM>
static bool MatchBoundsIsNotNullFilter(const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	return stats.has_not_null == true;
}

template <class TRANSFORM>
static bool MatchBoundsConjunctionAndFilter(const ConjunctionAndFilter &conjunction_and,
                                            const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	for (auto &child : conjunction_and.child_filters) {
		if (!MatchBoundsTemplated<TRANSFORM>(*child, stats, transform)) {
			return false;
		}
	}
	return true;
}

template <class TRANSFORM>
bool MatchBoundsTemplated(const TableFilter &filter, const IcebergPredicateStats &stats,
                          const IcebergTransform &transform) {
	//! TODO: support more filter types
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		return MatchBoundsConstantFilter<TRANSFORM>(constant_filter, stats, transform);
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and_filter = filter.Cast<ConjunctionAndFilter>();
		return MatchBoundsConjunctionAndFilter<TRANSFORM>(conjunction_and_filter, stats, transform);
	}
	case TableFilterType::IS_NULL: {
		//! FIXME: these are never hit, because it goes through ExpressionFilter instead?
		return MatchBoundsIsNullFilter<TRANSFORM>(stats, transform);
	}
	case TableFilterType::IS_NOT_NULL: {
		//! FIXME: these are never hit, because it goes through ExpressionFilter instead?
		return MatchBoundsIsNotNullFilter<TRANSFORM>(stats, transform);
	}
	case TableFilterType::EXPRESSION_FILTER: {
		//! Expressions can be arbitrarily complex, and we currently only support IS NULL/IS NOT NULL checks against the
		//! column itself, i.e. where the expression is a BOUND_OPERATOR with type OPERATOR_IS_NULL/_IS_NOT_NULL with a
		//! single child expression of type BOUND_REF.
		//!
		//! See duckdb/duckdb-iceberg#464
		auto &expression_filter = filter.Cast<ExpressionFilter>();
		auto &expr = *expression_filter.expr;

		if (expr.type != ExpressionType::OPERATOR_IS_NULL && expr.type != ExpressionType::OPERATOR_IS_NOT_NULL) {
			return true;
		}

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
		} else {
			D_ASSERT(expr.type == ExpressionType::OPERATOR_IS_NOT_NULL);
			return MatchBoundsIsNotNullFilter<TRANSFORM>(stats, transform);
		}
	}
	default:
		//! Conservative approach: we don't know what this is, just say it doesn't filter anything
		return true;
	}
}

bool IcebergPredicate::MatchBounds(const TableFilter &filter, const IcebergPredicateStats &stats,
                                   const IcebergTransform &transform) {
	switch (transform.Type()) {
	case IcebergTransformType::IDENTITY:
		return MatchBoundsTemplated<IdentityTransform>(filter, stats, transform);
	case IcebergTransformType::BUCKET:
		return true;
	case IcebergTransformType::TRUNCATE:
		return true;
	case IcebergTransformType::YEAR:
		return MatchBoundsTemplated<YearTransform>(filter, stats, transform);
	case IcebergTransformType::MONTH:
		return MatchBoundsTemplated<MonthTransform>(filter, stats, transform);
	case IcebergTransformType::DAY:
		return MatchBoundsTemplated<DayTransform>(filter, stats, transform);
	case IcebergTransformType::HOUR:
		return MatchBoundsTemplated<HourTransform>(filter, stats, transform);
	case IcebergTransformType::VOID:
		return true;
	default:
		throw InvalidConfigurationException("Transform '%s' not implemented", transform.RawType());
	}
}

} // namespace duckdb
