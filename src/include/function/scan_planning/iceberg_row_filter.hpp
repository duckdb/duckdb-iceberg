#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/table_filter_set.hpp"

namespace duckdb {

struct IcebergRowFilter {
	//! NOTE: Replace SQL transport with a versioned, typed expression using Iceberg field IDs.
	//! SQL replay rebinds names, casts and functions and can change meaning with session settings or engine versions.
	//! A typed format would preserve literal types and field identity without depending on SQL rebinding.
	static unique_ptr<Expression> Bind(ClientContext &context, const string &sql, const LogicalType &schema);
	static TableFilterSet TableFilters(ClientContext &context, const Expression &expression,
	                                   const vector<ColumnIndex> &columns, bool &always_false);
};

} // namespace duckdb
