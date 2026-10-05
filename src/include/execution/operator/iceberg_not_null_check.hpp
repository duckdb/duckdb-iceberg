//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/iceberg_not_null_check.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

#include "core/metadata/schema/iceberg_column_definition.hpp"

namespace duckdb {

struct IcebergNotNullCheck {
	//! Wraps `input`, the value written for `column`, in a check that fails write operation when
	//! a required field is NULL.
	static unique_ptr<Expression> Wrap(unique_ptr<Expression> input, const IcebergColumnDefinition &column,
	                                   const string &table_name);
};

} // namespace duckdb
