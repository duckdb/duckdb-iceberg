#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"

namespace duckdb {

struct IcebergDefault {
	static bool &InterpretStructNullAsEmpty();
};

class IcebergDefaultBinder {
public:
	IcebergDefaultBinder(ClientContext &context);

public:
	Value Evaluate(optional_ptr<const ParsedExpression> expr, const LogicalType &type);

private:
	ClientContext &context;
	shared_ptr<Binder> binder;
	ConstantBinder constant_binder;
};

struct IcebergDefaultProjectionResolver {
	static unique_ptr<Expression> ResolveDefault(ClientContext &context, const LogicalType &input_type,
	                                             const LogicalType &result_type, ColumnBinding binding,
	                                             const Expression &default_expr);
};

} // namespace duckdb
