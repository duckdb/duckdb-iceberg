#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/expression.hpp"
#include "planning/iceberg_multi_file_list.hpp"

#include "core/metadata/schema/iceberg_table_schema.hpp"

namespace duckdb {

struct LakeFormationFilterParseResult {
	unique_ptr<ParsedExpression> parsed_filter;
	unique_ptr<Expression> bound_filter;
	IcebergTableFilters table_filters;
};

LakeFormationFilterParseResult ParseLakeFormationRowFilter(ClientContext &context, const string &row_filter_sql,
                                                           const IcebergTableSchema &schema);

} // namespace duckdb
