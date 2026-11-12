#include "function/scan_planning/iceberg_row_filter.hpp"

#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"

namespace duckdb {

static void ValidateRowFilter(const ParsedExpression &expression) {
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF:
	case ExpressionClass::CONSTANT:
	case ExpressionClass::CAST:
	case ExpressionClass::COMPARISON:
	case ExpressionClass::CONJUNCTION:
	case ExpressionClass::BETWEEN:
	case ExpressionClass::OPERATOR:
		break;
	default:
		throw BinderException("Iceberg row_filter supports comparisons, Boolean operators, IN and null checks; "
		                      "subqueries, parameters and function calls are not supported");
	}
	ParsedExpressionIterator::EnumerateChildren(expression,
	                                            [](const ParsedExpression &child) { ValidateRowFilter(child); });
}

unique_ptr<Expression> IcebergRowFilter::Bind(ClientContext &context, const string &sql, const LogicalType &schema) {
	auto expressions = Parser(context).ParseExpressionList(sql);
	if (expressions.size() != 1) {
		throw BinderException("Iceberg row_filter requires exactly one Boolean expression");
	}
	ValidateRowFilter(*expressions[0]);
	auto binder = Binder::CreateBinder(context);
	vector<Identifier> names;
	vector<LogicalType> types;
	for (auto &field : StructType::GetChildTypes(schema)) {
		names.push_back(field.first);
		types.push_back(field.second);
	}
	binder->bind_context.AddGenericBinding(binder->GenerateTableIndex(), "iceberg_row", names, types);
	WhereBinder expression_binder(*binder, context);
	auto result = expression_binder.Bind(expressions[0]);
	if (!result->IsConsistent()) {
		throw BinderException("Iceberg row_filter must be deterministic");
	}
	return result;
}

TableFilterSet IcebergRowFilter::TableFilters(ClientContext &context, const Expression &expression,
                                              const vector<ColumnIndex> &columns, bool &always_false) {
	FilterCombiner combiner(context);
	always_false = false;
	vector<unique_ptr<Expression>> predicates;
	predicates.push_back(expression.Copy());
	LogicalFilter::SplitPredicates(predicates);
	for (auto &predicate : predicates) {
		if (combiner.AddFilter(std::move(predicate)) == FilterResult::UNSATISFIABLE) {
			always_false = true;
			return TableFilterSet();
		}
	}
	vector<FilterPushdownResult> results;
	return combiner.GenerateTableScanFilters(columns, results);
}

} // namespace duckdb
