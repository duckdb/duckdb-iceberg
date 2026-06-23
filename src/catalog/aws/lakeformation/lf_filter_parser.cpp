#include "catalog/aws/lakeformation/lf_filter_parser.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

namespace {

static unique_ptr<ParsedExpression> ExtractWhereExpression(const string &row_filter_sql) {
	Parser parser;
	auto query = StringUtil::Format("SELECT * FROM lf_filter_source WHERE %s", row_filter_sql);
	parser.ParseQuery(query);
	if (parser.statements.size() != 1) {
		throw ParserException("Lake Formation row filter could not be parsed: %s", row_filter_sql);
	}
	auto &statement = parser.statements[0];
	if (statement->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Lake Formation row filter must be a filter expression: %s", row_filter_sql);
	}
	auto &select_node = statement->Cast<SelectStatement>().node->Cast<SelectNode>();
	if (!select_node.where_clause) {
		throw ParserException("Lake Formation row filter is empty");
	}
	return std::move(select_node.where_clause);
}

} // namespace

LakeFormationFilterParseResult ParseLakeFormationRowFilter(ClientContext &context, const string &row_filter_sql,
                                                           const IcebergTableSchema &schema) {
	LakeFormationFilterParseResult result;
	if (row_filter_sql.empty()) {
		return result;
	}

	// LF filter expressions are SQL fragments (e.g. "id < 3"); wrap them so DuckDB's
	// parser and binder can treat them like a regular WHERE clause over table columns.
	result.parsed_filter = ExtractWhereExpression(row_filter_sql);

	vector<Identifier> names;
	vector<LogicalType> types;
	for (auto &col : schema.columns) {
		names.emplace_back(col->name);
		types.push_back(col->type);
	}

	auto binder = Binder::CreateBinder(context);
	binder->bind_context.AddGenericBinding(TableIndex(0), Identifier("lf_filter_source"), names, types);

	WhereBinder where_binder(*binder, context);
	auto bound_expr = where_binder.Bind(result.parsed_filter);

	result.bound_filter = std::move(bound_expr);
	if (!schema.columns.empty()) {
		result.table_filters.PushFilter(0, make_uniq<ExpressionFilter>(result.bound_filter->Copy()));
	}
	return result;
}

} // namespace duckdb
