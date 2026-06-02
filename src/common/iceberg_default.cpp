#include "common/iceberg_default.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

IcebergDefaultBinder::IcebergDefaultBinder(ClientContext &context)
    : context(context), binder(Binder::CreateBinder(context)), constant_binder(*binder, context, "DEFAULT") {
}

Value IcebergDefaultBinder::Evaluate(optional_ptr<const ParsedExpression> expr, const LogicalType &type) {
	if (!expr) {
		return Value(type);
	}
	auto expr_copy = expr->Copy();
	auto bound_expr = constant_binder.Bind(expr_copy, nullptr);
	if (!bound_expr->IsFoldable()) {
		throw NotImplementedException("Only foldable expressions are allowed as DEFAULT values");
	}
	return ExpressionExecutor::EvaluateScalar(context, *bound_expr, false).DefaultCastAs(type);
}

} // namespace duckdb
