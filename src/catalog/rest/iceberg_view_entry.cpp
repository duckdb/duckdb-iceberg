#include "catalog/rest/iceberg_view_entry.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/list.hpp"
#include "duckdb/parser/tableref/list.hpp"

namespace duckdb {

UnsupportedIcebergViewEntry::UnsupportedIcebergViewEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                         CreateViewInfo &info, string reason)
    : ViewCatalogEntry(catalog, schema, info), reason(std::move(reason)) {
}

const SelectStatement &UnsupportedIcebergViewEntry::GetQuery() {
	throw BinderException("Cannot query Iceberg view '%s': %s", name, reason);
}

void UnsupportedIcebergViewEntry::BindView(ClientContext &context, BindViewAction action) {
	GetQuery();
}

unique_ptr<CreateInfo> UnsupportedIcebergViewEntry::GetInfo() const {
	auto info = make_uniq<CreateViewInfo>(schema, name);
	info->sql = sql;
	info->aliases = aliases;
	return std::move(info);
}

string UnsupportedIcebergViewEntry::ToSQL() const {
	return sql;
}

unique_ptr<CatalogEntry> UnsupportedIcebergViewEntry::Copy(ClientContext &context) const {
	auto info = GetInfo();
	return make_uniq<UnsupportedIcebergViewEntry>(catalog, schema, info->Cast<CreateViewInfo>(), reason);
}

namespace {

bool NamesTableWithoutNamespace(QueryNode &node, identifier_set_t ctes);

//! Whether a subquery in the expression names a table without its namespace
bool ExpressionNamesTableWithoutNamespace(ParsedExpression &expr, const identifier_set_t &ctes) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY &&
	    NamesTableWithoutNamespace(*expr.Cast<SubqueryExpression>().SubqueryMutable()->node, ctes)) {
		return true;
	}
	bool found = false;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { found = found || ExpressionNamesTableWithoutNamespace(child, ctes); });
	return found;
}

//! Whether the FROM clause names a table without its namespace
bool RefNamesTableWithoutNamespace(TableRef &ref, const identifier_set_t &ctes) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &table_name = ref.Cast<BaseTableRef>().GetQualifiedName();
		return table_name.Path().size() == 1 && !ctes.count(table_name.Name());
	}
	case TableReferenceType::SUBQUERY:
		return NamesTableWithoutNamespace(*ref.Cast<SubqueryRef>().subquery->node, ctes);
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		return RefNamesTableWithoutNamespace(*join.left, ctes) || RefNamesTableWithoutNamespace(*join.right, ctes) ||
		       (join.condition && ExpressionNamesTableWithoutNamespace(*join.condition, ctes));
	}
	case TableReferenceType::PIVOT: {
		auto &pivot = ref.Cast<PivotRef>();
		bool found = RefNamesTableWithoutNamespace(*pivot.source, ctes);
		for (auto &aggregate : pivot.aggregates) {
			found = found || ExpressionNamesTableWithoutNamespace(*aggregate, ctes);
		}
		return found;
	}
	case TableReferenceType::TABLE_FUNCTION:
		return ExpressionNamesTableWithoutNamespace(*ref.Cast<TableFunctionRef>().function, ctes);
	case TableReferenceType::EXPRESSION_LIST: {
		bool found = false;
		for (auto &row : ref.Cast<ExpressionListRef>().values) {
			for (auto &value : row) {
				found = found || ExpressionNamesTableWithoutNamespace(*value, ctes);
			}
		}
		return found;
	}
	case TableReferenceType::EMPTY_FROM:
	case TableReferenceType::SHOW_REF:
	case TableReferenceType::COLUMN_DATA:
	case TableReferenceType::DELIM_GET:
		return false;
	default:
		// treat any other reference like a table without a namespace
		return true;
	}
}

//! Whether the query names a table without its namespace, given the CTE names in scope
bool NamesTableWithoutNamespace(QueryNode &node, identifier_set_t ctes) {
	// a CTE is visible after its definition, not in its own body or in the ones before it
	for (auto &cte : node.cte_map.map) {
		if (cte.second->query_node && NamesTableWithoutNamespace(*cte.second->query_node, ctes)) {
			return true;
		}
		ctes.insert(cte.first);
	}
	bool found = false;
	auto check = [&](unique_ptr<ParsedExpression> &expr) {
		found = found || (expr && ExpressionNamesTableWithoutNamespace(*expr, ctes));
	};
	ParsedExpressionIterator::EnumerateQueryNodeModifiers(node, check);
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		for (auto &expr : select.select_list) {
			check(expr);
		}
		for (auto &expr : select.groups.group_expressions) {
			check(expr);
		}
		check(select.where_clause);
		check(select.having);
		check(select.qualify);
		return found || RefNamesTableWithoutNamespace(*select.from_table, ctes);
	}
	case QueryNodeType::SET_OPERATION_NODE:
		for (auto &child : node.Cast<SetOperationNode>().children) {
			found = found || NamesTableWithoutNamespace(*child, ctes);
		}
		return found;
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &cte = node.Cast<RecursiveCTENode>();
		found = found || NamesTableWithoutNamespace(*cte.left, ctes);
		// only the part after UNION sees the CTE itself
		ctes.insert(cte.ctename);
		return found || NamesTableWithoutNamespace(*cte.right, ctes);
	}
	default:
		// treat any other query like one that names a table without a namespace
		return true;
	}
}

//! Whether the query's output columns come from *
bool OutputExpandsStar(QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		bool found = false;
		for (auto &expr : node.Cast<SelectNode>().select_list) {
			ParsedExpressionIterator::VisitExpression<StarExpression>(*expr,
			                                                          [&](const StarExpression &) { found = true; });
		}
		return found;
	}
	case QueryNodeType::SET_OPERATION_NODE:
		for (auto &child : node.Cast<SetOperationNode>().children) {
			if (OutputExpandsStar(*child)) {
				return true;
			}
		}
		return false;
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &cte = node.Cast<RecursiveCTENode>();
		return OutputExpandsStar(*cte.left) || OutputExpandsStar(*cte.right);
	}
	default:
		// treat any other query like *
		return true;
	}
}

} // namespace

IcebergViewEntry::IcebergViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info)
    : ViewCatalogEntry(catalog, schema, info), columns_from_star(query && OutputExpandsStar(*query->node)) {
}

const SelectStatement &IcebergViewEntry::GetQuery() {
	if (query && NamesTableWithoutNamespace(*query->node, identifier_set_t())) {
		throw NotImplementedException("Querying Iceberg view '%s' is not supported: its query names a table without a "
		                              "namespace. Qualify the table names in the view's query with their namespace",
		                              name.GetIdentifierName());
	}
	return ViewCatalogEntry::GetQuery();
}

void IcebergViewEntry::UpdateBinding(const vector<LogicalType> &types, const vector<Identifier> &names) {
	if (columns_from_star) {
		// the stored names apply by position, so * must still give the stored columns first
		auto query_names = names;
		QueryResult::DeduplicateColumns(query_names);
		for (idx_t i = 0; i < aliases.size() && i < query_names.size(); i++) {
			if (query_names[i] != aliases[i]) {
				throw NotImplementedException("Querying Iceberg view '%s' is not supported: its query selects *, and "
				                              "column %d of the query is '%s' where the view stores '%s'. Recreate "
				                              "the view, listing its columns instead of *",
				                              name.GetIdentifierName(), i + 1, query_names[i].GetIdentifierName(),
				                              aliases[i].GetIdentifierName());
			}
		}
	}
	ViewCatalogEntry::UpdateBinding(types, names);
}

unique_ptr<CatalogEntry> IcebergViewEntry::Copy(ClientContext &context) const {
	auto info = GetInfo();
	return make_uniq<IcebergViewEntry>(catalog, schema, info->Cast<CreateViewInfo>());
}

void IcebergViewEntry::CheckCreate(CreateViewInfo &info) {
	if (!info.query) {
		return;
	}
	auto view_name = info.GetViewName().GetIdentifierName();
	if (NamesTableWithoutNamespace(*info.query->node, identifier_set_t())) {
		throw NotImplementedException("Creating Iceberg view '%s' is not supported: its query names a table without a "
		                              "namespace. Qualify the table names in the view's query with their namespace",
		                              view_name);
	}
	if (info.aliases.empty() || !OutputExpandsStar(*info.query->node)) {
		return;
	}
	// compare the names the view would store with the names * gives
	auto stored_names = info.names;
	for (idx_t i = 0; i < info.aliases.size() && i < stored_names.size(); i++) {
		if (!info.aliases[i].empty()) {
			stored_names[i] = info.aliases[i];
		}
	}
	QueryResult::DeduplicateColumns(stored_names);
	auto query_names = info.names;
	QueryResult::DeduplicateColumns(query_names);
	for (idx_t i = 0; i < stored_names.size(); i++) {
		if (query_names[i] != stored_names[i]) {
			throw NotImplementedException("Creating Iceberg view '%s' is not supported: its column list renames the "
			                              "columns of *. List the columns in the view's query instead of *",
			                              view_name);
		}
	}
}

} // namespace duckdb
