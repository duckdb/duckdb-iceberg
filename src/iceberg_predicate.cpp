#include "iceberg_predicate.hpp"
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
		auto &expression_filter = filter.Cast<ExpressionFilter>();
		auto &expr = *expression_filter.expr;
		if (expr.type == ExpressionType::OPERATOR_IS_NULL) {
			return MatchBoundsIsNullFilter<TRANSFORM>(stats, transform);
		}
		if (expr.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
			return MatchBoundsIsNotNullFilter<TRANSFORM>(stats, transform);
		}
		//! Any other expression can not be filtered
		return true;
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
