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

//! Whether the query names a table without its namespace
bool NamesTableWithoutNamespace(QueryNode &node, identifier_set_t ctes) {
	for (auto &cte : node.cte_map.map) {
		ctes.insert(cte.first);
	}
	bool found = false;
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    node,
	    [&](unique_ptr<ParsedExpression> &child) {
		    ParsedExpressionIterator::VisitExpressionMutable<SubqueryExpression>(
		        *child, [&](SubqueryExpression &subquery) {
			        found = NamesTableWithoutNamespace(*subquery.SubqueryMutable()->node, ctes) || found;
		        });
	    },
	    [&](TableRef &ref) {
		    if (ref.type == TableReferenceType::BASE_TABLE) {
			    auto &table_name = ref.Cast<BaseTableRef>().GetQualifiedName();
			    found = found || (table_name.Path().size() == 1 && !ctes.count(table_name.Name()));
		    }
	    });
	return found;
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

} // namespace duckdb
