#include "planning/pruning/iceberg_table_filter.hpp"

#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

namespace {

static void NormalizeOptionalFilterCallbacks(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		optional_ptr<Expression> child_filter;
		if (func.GetChildren().size() == 1 && func.BindInfo()) {
			auto &type = func.GetChildren()[0]->GetReturnType();
			if (func.Function().GetName() == OptionalFilterScalarFun::NAME) {
				func.FunctionMutable().SetCallbacks(OptionalFilterScalarFun::GetFunction(type).GetCallbacks());
				child_filter = func.BindInfo()->Cast<OptionalFilterFunctionData>().child_filter_expr.get();
			} else if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME) {
				func.FunctionMutable().SetCallbacks(
				    SelectivityOptionalFilterScalarFun::GetFunction(type).GetCallbacks());
				child_filter = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>().child_filter_expr.get();
			}
		}
		if (child_filter) {
			NormalizeOptionalFilterCallbacks(*child_filter);
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { NormalizeOptionalFilterCallbacks(child); });
}

static unique_ptr<Expression> CreateReferenceExpression(const LogicalType &type) {
	return make_uniq<BoundReferenceExpression>(type, 0ULL);
}

static void AppendColumnPath(const ColumnIndex &column_index, vector<idx_t> &path) {
	for (auto &child_index : column_index.GetChildIndexes()) {
		path.push_back(child_index.GetPrimaryIndex());
		AppendColumnPath(child_index, path);
	}
}

static vector<idx_t> GetColumnPath(const ColumnIndex &column_index) {
	column_index.VerifySinglePath();
	vector<idx_t> path;
	AppendColumnPath(column_index, path);
	return path;
}

static bool TryGetFilterPath(const Expression &expr, vector<idx_t> &path) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return true;
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		idx_t child_idx;
		if (!TryGetStructExtractChildIndex(func, child_idx) || func.GetChildren().empty()) {
			return false;
		}
		if (!TryGetFilterPath(*func.GetChildren()[0], path)) {
			return false;
		}
		path.push_back(child_idx);
		return true;
	}
	default:
		return false;
	}
}

enum class FilterPathMatch : uint8_t { NONE, MATCH, OTHER };

static FilterPathMatch GetFilterPathMatch(const Expression &expr, const vector<idx_t> &path) {
	vector<idx_t> expr_path;
	if (TryGetFilterPath(expr, expr_path)) {
		return expr_path == path ? FilterPathMatch::MATCH : FilterPathMatch::OTHER;
	}
	auto result = FilterPathMatch::NONE;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (result == FilterPathMatch::OTHER) {
			return;
		}
		auto child_result = GetFilterPathMatch(child, path);
		if (child_result == FilterPathMatch::OTHER) {
			result = FilterPathMatch::OTHER;
		} else if (child_result == FilterPathMatch::MATCH) {
			result = FilterPathMatch::MATCH;
		}
	});
	return result;
}

static bool MatchesFilterPath(const Expression &expr, const vector<idx_t> &path) {
	vector<idx_t> expr_path;
	return TryGetFilterPath(expr, expr_path) && expr_path == path;
}

static void ReplaceFilterPathExpressions(unique_ptr<Expression> &expr, const vector<idx_t> &path) {
	if (MatchesFilterPath(*expr, path)) {
		expr = CreateReferenceExpression(expr->GetReturnType());
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { ReplaceFilterPathExpressions(child, path); });
}

static unique_ptr<Expression> ExtractFilterExpressionForPath(const Expression &expr, const vector<idx_t> &path) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr ? ExtractFilterExpressionForPath(*data.child_filter_expr, path) : nullptr;
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr ? ExtractFilterExpressionForPath(*data.child_filter_expr, path) : nullptr;
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		auto result = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &child : conjunction.GetChildren()) {
			auto extracted_child = ExtractFilterExpressionForPath(*child, path);
			if (extracted_child) {
				result->GetChildrenMutable().push_back(std::move(extracted_child));
			}
		}
		if (result->GetChildren().empty()) {
			return nullptr;
		}
		if (result->GetChildren().size() == 1) {
			return std::move(result->GetChildrenMutable()[0]);
		}
		return std::move(result);
	}
	if (GetFilterPathMatch(expr, path) != FilterPathMatch::MATCH) {
		return nullptr;
	}
	auto result = expr.Copy();
	ReplaceFilterPathExpressions(result, path);
	return result;
}

} // namespace

bool IcebergTableFilters::FiltersEqual(const ExpressionFilter &left, const ExpressionFilter &right) {
	if (left.Equals(right)) {
		return true;
	}
	if (left.column_indexes != right.column_indexes) {
		return false;
	}
	// Loadable extensions can have their own copies of DuckDB's optional-filter callbacks.
	// Normalize only those callbacks, preserving all predicate, type, and bind-data comparisons.
	auto left_copy = left.Copy();
	auto right_copy = right.Copy();
	NormalizeOptionalFilterCallbacks(*left_copy->expr);
	NormalizeOptionalFilterCallbacks(*right_copy->expr);
	return left_copy->Equals(*right_copy);
}

unique_ptr<ExpressionFilter> IcebergTableFilters::GetFilterForColumnIndex(const ColumnIndex &column_index) const {
	auto filter = TryGetFilterByColumnIndex(column_index);
	if (!filter && column_index.HasChildren()) {
		// Filters on struct fields can be registered for the top-level column. The path extraction below ensures
		// that we only return the part of the parent filter that targets the requested field.
		filter = TryGetFilterByColumnIndex(ColumnIndex(column_index.GetPrimaryIndex()));
	}
	if (!filter) {
		return nullptr;
	}

	auto path = GetColumnPath(column_index);
	if (path.empty()) {
		return filter->Copy();
	}

	auto child_expr = ExtractFilterExpressionForPath(*filter->expr, path);
	if (!child_expr) {
		return nullptr;
	}
	return make_uniq<ExpressionFilter>(std::move(child_expr));
}

} // namespace duckdb
