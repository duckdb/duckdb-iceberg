#include "planning/pruning/iceberg_predicate.hpp"

#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"

namespace duckdb {

namespace {

struct BoundExpressionReplacer : public LogicalOperatorVisitor {
public:
	BoundExpressionReplacer(const Value &val) : val(val) {
	}

public:
	unique_ptr<Expression> VisitReplace(BoundReferenceExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (expr.Index() != 0) {
			return nullptr;
		}
		auto &return_type = expr.GetReturnType();
		return make_uniq<BoundConstantExpression>(val.DefaultCastAs(return_type, true));
	}

public:
	const Value &val;
};

} // namespace

static bool IsNaN(const Value &value) {
	switch (value.type().id()) {
	case LogicalTypeId::FLOAT:
		return Value::IsNan(value.GetValue<float>());
	case LogicalTypeId::DOUBLE:
		return Value::IsNan(value.GetValue<double>());
	default:
		return false;
	}
}

static bool NaNMayMatch(const Value &constant, ExpressionType comparison_type, const IcebergPredicateStats &stats) {
	auto type_id = constant.type().id();
	if ((stats.has_nan && !*stats.has_nan) || (type_id != LogicalTypeId::FLOAT && type_id != LogicalTypeId::DOUBLE)) {
		return false;
	}
	switch (comparison_type) {
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return true;
	case ExpressionType::COMPARE_EQUAL:
		return IsNaN(constant);
	default:
		return false;
	}
}

template <class TRANSFORM>
static bool MatchBoundsConstantTemplated(const Value &constant, ExpressionType comparison_type,
                                         const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	auto constant_value = TRANSFORM::ApplyTransform(constant, transform);
	if (constant_value.IsNull()) {
		//! Comparison with NULL is never true here
		return false;
	}

	if (stats.BoundsAreNull()) {
		// bounds are actually null, expression is not a null comparison expression
		// those are handled in MatchBoundsExpression
		// So we can return false since no remaining expression type will match a null value
		D_ASSERT(comparison_type != ExpressionType::OPERATOR_IS_NOT_NULL);
		D_ASSERT(comparison_type != ExpressionType::OPERATOR_IS_NULL);
		D_ASSERT(comparison_type != ExpressionType::COMPARE_DISTINCT_FROM);
		D_ASSERT(comparison_type != ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		return false;
	}

	if (!stats.has_not_null && comparison_type != ExpressionType::COMPARE_DISTINCT_FROM &&
	    comparison_type != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		// has_not_null is false when every row of this column is NULL. Ordinary comparisons
		// against a constant cannot match NULL so the file can be pruned. IS [NOT] DISTINCT FROM
		// must consider NULL so cannot be pruned.
		return false;
	}

	if (!stats.upper_bound || !stats.lower_bound) {
		// we do not have upper or lower bounds, assume the file matches.
		return true;
	}

	if (NaNMayMatch(constant_value, comparison_type, stats)) {
		// bounds exclude NaN, but the file may hold NaN rows that match the comparison.
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

static bool MatchBoundsConstant(const Value &constant, ExpressionType comparison_type,
                                const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	switch (transform.Type()) {
	case IcebergTransformType::IDENTITY:
		return MatchBoundsConstantTemplated<IdentityTransform>(constant, comparison_type, stats, transform);
	case IcebergTransformType::BUCKET:
		return MatchBoundsConstantTemplated<BucketTransform>(constant, comparison_type, stats, transform);
	case IcebergTransformType::TRUNCATE:
		return MatchBoundsConstantTemplated<TruncateTransform>(constant, comparison_type, stats, transform);
	case IcebergTransformType::YEAR:
		return MatchBoundsConstantTemplated<YearTransform>(constant, comparison_type, stats, transform);
	case IcebergTransformType::MONTH:
		return MatchBoundsConstantTemplated<MonthTransform>(constant, comparison_type, stats, transform);
	case IcebergTransformType::DAY:
		return MatchBoundsConstantTemplated<DayTransform>(constant, comparison_type, stats, transform);
	case IcebergTransformType::HOUR:
		return MatchBoundsConstantTemplated<HourTransform>(constant, comparison_type, stats, transform);
	case IcebergTransformType::VOID:
		return true;
	default:
		throw InvalidConfigurationException("Transform '%s' not implemented", transform.RawType());
	}
}

static bool MatchBoundsIsNullFilter(const IcebergPredicateStats &stats) {
	return stats.has_null == true;
}

static bool MatchBoundsIsNotNullFilter(const IcebergPredicateStats &stats) {
	return stats.has_not_null == true;
}

//! variant_comparator keys start with a byte that ranks the value's type (all numbers share one rank)
static bool SameVariantTypeRank(const Value &key, const Value &other_key) {
	if (key.type().id() != LogicalTypeId::BLOB || other_key.type().id() != LogicalTypeId::BLOB) {
		return true;
	}
	auto &a = StringValue::Get(key);
	auto &b = StringValue::Get(other_key);
	return !a.empty() && !b.empty() && a[0] == b[0];
}

static bool TryEvaluateVariantScalarBound(ClientContext &context, const Expression &expr, Value &result) {
	auto &comparator = expr.Cast<BoundFunctionExpression>();
	D_ASSERT(comparator.Function().GetName() == "variant_comparator");
	D_ASSERT(comparator.GetChildren().size() == 1);

	Value bound;
	if (!ExpressionExecutor::TryEvaluateScalar(context, *comparator.GetChildren()[0], bound) || bound.IsNull()) {
		return false;
	}
	D_ASSERT(bound.type().id() == LogicalTypeId::VARIANT);
	Vector bound_vector(bound, count_t(1));
	VariantIterator iterator(bound_vector);
	switch (iterator.Root(0).GetTypeId()) {
	case VariantLogicalType::VARIANT_NULL:
	case VariantLogicalType::OBJECT:
	case VariantLogicalType::ARRAY:
		// Missing fields and reconstructed containers are not scalar bounds.
		return false;
	default:
		return ExpressionExecutor::TryEvaluateScalar(context, expr, result);
	}
}

bool MatchVariantBounds(ClientContext &context, ExpressionType comparison_type, const Expression &left,
                        const Expression &right, const IcebergPredicateStats &stats,
                        const IcebergTransform &transform) {
	if (comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
	    comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
		// Iceberg uses the same primitive bounds for a scalar and for the elements of an array.
		// An ARRAY variant sorts after every primitive, so it may match even when its elements do not.
		// Without the field's actual type, these bounds cannot rule out either comparison.
		return true;
	}
	if (!stats.lower_bound || !stats.upper_bound) {
		return true;
	}
	BoundExpressionReplacer lower_replacer(*stats.lower_bound);
	BoundExpressionReplacer upper_replacer(*stats.upper_bound);
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
	if (!TryEvaluateVariantScalarBound(context, *lower_copy, transformed_lower_bound)) {
		return true;
	}
	if (!TryEvaluateVariantScalarBound(context, *upper_copy, transformed_upper_bound)) {
		return true;
	}
	if (transformed_lower_bound.IsNull() || transformed_upper_bound.IsNull()) {
		//! The bounds have no entry for this path, so nothing is known about its values
		return true;
	}
	if (!SameVariantTypeRank(transformed_lower_bound, right_constant) ||
	    !SameVariantTypeRank(transformed_upper_bound, right_constant)) {
		//! Bounds of another type say nothing about values of the filtered type
		return true;
	}
	IcebergPredicateStats transformed_stats(stats);
	transformed_stats.lower_bound = transformed_lower_bound;
	transformed_stats.upper_bound = transformed_upper_bound;

	return MatchBoundsConstant(right_constant, comparison_type, transformed_stats, transform);
}

static bool IsDirectReference(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return true;
	default: {
		return false;
	}
	}
}

static bool IsVariantExtract(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &variant_extract = expr.Cast<BoundFunctionExpression>();
	if (variant_extract.Function().GetName() != "variant_extract") {
		return false;
	}
	if (variant_extract.GetChildren().empty()) {
		return false;
	}
	return true;
}

static bool IsVariantReference(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &variant_comparator_func = expr.Cast<BoundFunctionExpression>();
	if (variant_comparator_func.Function().GetName() != "variant_comparator") {
		return false;
	}
	if (variant_comparator_func.GetChildren().size() != 1) {
		return false;
	}

	reference<const Expression> current_expr(*variant_comparator_func.GetChildren()[0]);
	while (IsVariantExtract(current_expr)) {
		auto &func = current_expr.get().Cast<BoundFunctionExpression>();
		current_expr = *func.GetChildren()[0];
	}
	return IsDirectReference(current_expr);
}

static bool MatchBoundsExpression(ClientContext &context, const unique_ptr<Expression> &expr_p,
                                  const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	auto &expr = *expr_p;
	if (BoundComparisonExpression::IsComparison(expr)) {
		auto &compare_expr = expr.Cast<BoundFunctionExpression>();
		auto comparison_type = compare_expr.GetExpressionType();
		auto &left = BoundComparisonExpression::Left(compare_expr);
		auto &right = BoundComparisonExpression::Right(compare_expr);
		const bool right_is_const = right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
		const bool left_is_const = left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;

		const bool left_is_ref = IsDirectReference(left);
		const bool right_is_ref = IsDirectReference(right);

		const bool is_identity = transform.Type() == IcebergTransformType::IDENTITY;

		if (right_is_const && left_is_const) {
			return true;
		} else if (right_is_const) {
			if (left_is_ref) {
				return MatchBoundsConstant(right.Cast<BoundConstantExpression>().GetValue(), comparison_type, stats,
				                           transform);
			} else if (is_identity && IsVariantReference(left)) {
				return MatchVariantBounds(context, comparison_type, left, right, stats, transform);
			}
		} else if (left_is_const) {
			if (right_is_ref) {
				return MatchBoundsConstant(left.Cast<BoundConstantExpression>().GetValue(),
				                           FlipComparisonExpression(comparison_type), stats, transform);
			} else if (is_identity && IsVariantReference(right)) {
				return MatchVariantBounds(context, FlipComparisonExpression(comparison_type), right, left, stats,
				                          transform);
			}
		}
		return true;
	}

	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		if (conjunction.GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
			return true;
		}
		for (auto &child : conjunction.GetChildren()) {
			if (!MatchBoundsExpression(context, child, stats, transform)) {
				return false;
			}
		}
		return true;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &bound_operator_expr = expr.Cast<BoundOperatorExpression>();
		switch (expr.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			//! Expressions can be arbitrarily complex, and we currently only support IS NULL/IS NOT NULL checks against
			//! the column itself, i.e. where the expression is a BOUND_OPERATOR with type OPERATOR_IS_NULL/_IS_NOT_NULL
			//! with a single child expression of type BOUND_REF.
			//!
			//! See duckdb/duckdb-iceberg#464
			if (bound_operator_expr.GetChildren().size() != 1 ||
			    !IsDirectReference(*bound_operator_expr.GetChildren()[0])) {
				//! We can't evaluate expressions that aren't direct column references
				return true;
			}
			if (expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL) {
				return MatchBoundsIsNullFilter(stats);
			}
			D_ASSERT(expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL);
			return MatchBoundsIsNotNullFilter(stats);
		}
		case ExpressionType::COMPARE_IN: {
			if (bound_operator_expr.GetChildren().empty() ||
			    !IsDirectReference(*bound_operator_expr.GetChildren()[0])) {
				return true;
			}
			for (idx_t i = 1; i < bound_operator_expr.GetChildren().size(); i++) {
				if (bound_operator_expr.GetChildren()[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					return true;
				}
				auto &value = bound_operator_expr.GetChildren()[i]->Cast<BoundConstantExpression>().GetValue();
				if (MatchBoundsConstant(value, ExpressionType::COMPARE_EQUAL, stats, transform)) {
					return true;
				}
			}
			return false;
		}
		default:
			return true;
		}
	}
	case ExpressionClass::BOUND_FUNCTION: {
		if (stats.geometry_stats) {
			auto result = ExpressionFilter::CheckExpressionStatistics(expr, *stats.geometry_stats);
			return result != FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}

		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			if (data.child_filter_expr) {
				return MatchBoundsExpression(context, data.child_filter_expr, stats, transform);
			}
			//! child filter wasn't populated (yet?) for some reason, just be conservative
			return true;
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			if (data.child_filter_expr) {
				return MatchBoundsExpression(context, data.child_filter_expr, stats, transform);
			}
			//! child filter wasn't populated (yet?) for some reason, just be conservative
			return true;
		}
		return true;
	}
	default:
		//! Conservative approach: we don't know what this is, just say it doesn't filter anything
		return true;
	}
}

bool IcebergPredicate::MatchBounds(ClientContext &context, const ExpressionFilter &filter,
                                   const IcebergPredicateStats &stats, const IcebergTransform &transform) {
	return MatchBoundsExpression(context, filter.expr, stats, transform);
}

} // namespace duckdb
