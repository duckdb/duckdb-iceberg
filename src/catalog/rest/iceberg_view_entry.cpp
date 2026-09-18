#include "catalog/rest/iceberg_view_entry.hpp"
#include "duckdb/common/exception/binder_exception.hpp"

#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"

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

class ViewReferenceQualifier {
public:
	ViewReferenceQualifier(const Identifier &catalog, const Identifier &schema) : catalog(catalog), schema(schema) {
	}

	void Query(QueryNode &node, identifier_set_t ctes) {
		for (auto &cte : node.cte_map.map) {
			ctes.insert(cte.first);
		}
		if (node.type == QueryNodeType::RECURSIVE_CTE_NODE) {
			ctes.insert(node.Cast<RecursiveCTENode>().ctename);
		}
		for (auto &cte : node.cte_map.map) {
			auto definition_ctes = ctes;
			// A non-recursive CTE may read a real table with its own name.
			// RecursiveCTENode adds its own name back for the recursive term.
			definition_ctes.erase(cte.first);
			Query(*cte.second->query_node, std::move(definition_ctes));
			for (auto &expr : cte.second->key_targets) {
				Expression(expr, ctes);
			}
			for (auto &expr : cte.second->payload_aggregates) {
				Expression(expr, ctes);
			}
		}
		auto visit_expr = [&](unique_ptr<ParsedExpression> &expr) {
			Expression(expr, ctes);
		};
		ParsedExpressionIterator::EnumerateQueryNodeModifiers(node, visit_expr);
		switch (node.type) {
		case QueryNodeType::SELECT_NODE: {
			auto &select = node.Cast<SelectNode>();
			for (auto &expr : select.select_list) {
				Expression(expr, ctes);
			}
			for (auto &expr : select.groups.group_expressions) {
				Expression(expr, ctes);
			}
			Expression(select.where_clause, ctes);
			Expression(select.having, ctes);
			Expression(select.qualify, ctes);
			Table(*select.from_table, ctes);
			break;
		}
		case QueryNodeType::SET_OPERATION_NODE:
			for (auto &child : node.Cast<SetOperationNode>().children) {
				Query(*child, ctes);
			}
			break;
		case QueryNodeType::RECURSIVE_CTE_NODE: {
			auto &recursive = node.Cast<RecursiveCTENode>();
			Query(*recursive.left, ctes);
			Query(*recursive.right, ctes);
			for (auto &expr : recursive.key_targets) {
				Expression(expr, ctes);
			}
			break;
		}
		default:
			throw NotImplementedException("This query form is not supported in an Iceberg view");
		}
	}

private:
	void Expression(unique_ptr<ParsedExpression> &expr, const identifier_set_t &ctes) {
		if (!expr) {
			return;
		}
		if (expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
			auto &subquery = expr->Cast<SubqueryExpression>();
			Query(*subquery.SubqueryMutable()->node, ctes);
			Expression(subquery.GetChildMutable(), ctes);
			return;
		}
		ParsedExpressionIterator::EnumerateChildren(
		    *expr, [&](unique_ptr<ParsedExpression> &child) { Expression(child, ctes); });
	}

	void Table(TableRef &ref, const identifier_set_t &ctes) {
		switch (ref.type) {
		case TableReferenceType::BASE_TABLE: {
			auto &table = ref.Cast<BaseTableRef>();
			auto &name = table.GetQualifiedName();
			if (name.Path().size() == 1 && !ctes.count(name.Name())) {
				if (schema.empty()) {
					throw NotImplementedException("Unqualified table references with an empty Iceberg view namespace "
					                              "are not supported");
				}
				table.SetQualifiedName(catalog, schema, name.Name());
			} else if (name.Path().size() == 2) {
				table.SetQualifiedName(name.WithCatalog(catalog));
			}
			break;
		}
		case TableReferenceType::SUBQUERY:
			Query(*ref.Cast<SubqueryRef>().subquery->node, ctes);
			break;
		case TableReferenceType::JOIN: {
			auto &join = ref.Cast<JoinRef>();
			Table(*join.left, ctes);
			Table(*join.right, ctes);
			Expression(join.condition, ctes);
			break;
		}
		case TableReferenceType::EXPRESSION_LIST:
			for (auto &row : ref.Cast<ExpressionListRef>().values) {
				for (auto &expr : row) {
					Expression(expr, ctes);
				}
			}
			break;
		case TableReferenceType::TABLE_FUNCTION: {
			auto &function = ref.Cast<TableFunctionRef>();
			Expression(function.function, ctes);
			if (function.subquery) {
				Query(*function.subquery->node, ctes);
			}
			break;
		}
		case TableReferenceType::PIVOT: {
			auto &pivot = ref.Cast<PivotRef>();
			Table(*pivot.source, ctes);
			for (auto &expr : pivot.aggregates) {
				Expression(expr, ctes);
			}
			for (auto &column : pivot.pivots) {
				for (auto &expr : column.pivot_expressions) {
					Expression(expr, ctes);
				}
				for (auto &entry : column.entries) {
					Expression(entry.expr, ctes);
				}
				if (column.subquery) {
					Query(*column.subquery, ctes);
				}
			}
			break;
		}
		case TableReferenceType::EMPTY_FROM:
			break;
		default:
			throw NotImplementedException("This table reference is not supported in an Iceberg view");
		}
	}

	Identifier catalog;
	Identifier schema;
};

void QualifyIcebergView(SelectStatement &query, const rest_api_objects::ViewVersion &version,
                        const Identifier &owning_catalog) {
	auto catalog = version.default_catalog ? Identifier(*version.default_catalog) : owning_catalog;
	auto schema = Identifier(StringUtil::Join(version.default_namespace.value, "."));
	ViewReferenceQualifier(catalog, schema).Query(*query.node, {});
}

} // namespace duckdb
