#include "catalog/rest/iceberg_view_entry.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/main/client_context.hpp"
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

//! The table names a view's query reads, and the names of the CTEs it defines
struct ViewTableNames {
	vector<reference<BaseTableRef>> tables;
	identifier_set_t ctes;
};

void CollectTableNames(QueryNode &node, ViewTableNames &result);

void CollectTableNames(ParsedExpression &expr, ViewTableNames &result) {
	ParsedExpressionIterator::VisitExpressionMutable<SubqueryExpression>(expr, [&](SubqueryExpression &subquery) {
		CollectTableNames(*subquery.SubqueryMutable()->node, result);
		if (subquery.GetChild()) {
			CollectTableNames(*subquery.GetChildMutable(), result);
		}
	});
}

void CollectTableNames(TableRef &ref, ViewTableNames &result) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
		result.tables.push_back(ref.Cast<BaseTableRef>());
		break;
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		CollectTableNames(*join.left, result);
		CollectTableNames(*join.right, result);
		if (join.condition) {
			CollectTableNames(*join.condition, result);
		}
		break;
	}
	case TableReferenceType::SUBQUERY:
		CollectTableNames(*ref.Cast<SubqueryRef>().subquery->node, result);
		break;
	case TableReferenceType::TABLE_FUNCTION: {
		auto &function = ref.Cast<TableFunctionRef>().function;
		if (function) {
			CollectTableNames(*function, result);
		}
		break;
	}
	case TableReferenceType::PIVOT:
		CollectTableNames(*ref.Cast<PivotRef>().source, result);
		break;
	default:
		break;
	}
}

void CollectTableNames(QueryNode &node, ViewTableNames &result) {
	for (auto &cte : node.cte_map.map) {
		result.ctes.insert(cte.first);
		if (cte.second->query_node) {
			CollectTableNames(*cte.second->query_node, result);
		}
	}
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		for (auto &expr : select.select_list) {
			CollectTableNames(*expr, result);
		}
		if (select.from_table) {
			CollectTableNames(*select.from_table, result);
		}
		if (select.where_clause) {
			CollectTableNames(*select.where_clause, result);
		}
		for (auto &group : select.groups.group_expressions) {
			CollectTableNames(*group, result);
		}
		if (select.having) {
			CollectTableNames(*select.having, result);
		}
		if (select.qualify) {
			CollectTableNames(*select.qualify, result);
		}
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE:
		for (auto &child : node.Cast<SetOperationNode>().children) {
			CollectTableNames(*child, result);
		}
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &cte = node.Cast<RecursiveCTENode>();
		result.ctes.insert(cte.ctename);
		CollectTableNames(*cte.left, result);
		CollectTableNames(*cte.right, result);
		break;
	}
	default:
		break;
	}
}

//! Whether the output columns of a query come from expanding *, so they follow the columns of the tables it reads
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
		// check the names of any other kind of query
		return true;
	}
}

} // namespace

IcebergViewEntry::IcebergViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info,
                                   ClientContext &context)
    : ViewCatalogEntry(catalog, schema, info), reader(context.shared_from_this()),
      columns_from_star(query && OutputExpandsStar(*query->node)) {
}

void IcebergViewEntry::CheckTableNames(ClientContext &context) {
	ViewTableNames names;
	CollectTableNames(*query->node, names);
	//! A table name that is a single identifier refers to the view's default namespace, which is the view's own
	//! namespace here. The binder searches the temporary schema before it, and the reader's own search path after it,
	//! so look each such name up the way the binder does, and refuse when it would be read from somewhere else.
	vector<CatalogSearchEntry> search_path;
	search_path.emplace_back(ParentCatalog().GetName(), ParentSchema().name);
	search_path.emplace_back(ParentCatalog().GetName(), Identifier::InvalidSchema(), true);
	CatalogEntryRetriever retriever(context);
	retriever.SetSearchPath(std::move(search_path));
	for (auto &table : names.tables) {
		auto &table_name = table.get().GetQualifiedName();
		if (table_name.Path().size() != 1 || names.ctes.count(table_name.Name())) {
			continue;
		}
		auto entry =
		    retriever.GetEntry(EntryLookupInfo(CatalogType::TABLE_ENTRY, table_name), OnEntryNotFound::RETURN_NULL);
		if (!entry ||
		    (&entry->ParentCatalog() == &ParentCatalog() && entry->ParentSchema().name == ParentSchema().name)) {
			// read from the view's namespace, or found nowhere: the binder reports the missing table, or reads a file
			continue;
		}
		auto found = entry->ParentCatalog().GetName() + "." + entry->ParentSchema().name + "." + entry->name;
		throw BinderException(
		    "Cannot query Iceberg view '%s': table '%s' would be read from '%s' instead of the view's namespace '%s'",
		    name.GetIdentifierName(), table_name.Name().GetIdentifierName(), found,
		    ParentSchema().name.GetIdentifierName());
	}
}

const SelectStatement &IcebergViewEntry::GetQuery() {
	auto context = reader.lock();
	if (context) {
		CheckTableNames(*context);
	}
	return ViewCatalogEntry::GetQuery();
}

void IcebergViewEntry::UpdateBinding(const vector<LogicalType> &types, const vector<Identifier> &names) {
	if (columns_from_star) {
		//! The view's stored columns name its query's columns by position. Columns that come from * follow the tables
		//! the query reads, so they must still be the stored columns, in the same order. Columns added after them are
		//! returned under their own names.
		auto query_names = names;
		QueryResult::DeduplicateColumns(query_names);
		for (idx_t i = 0; i < aliases.size() && i < query_names.size(); i++) {
			if (query_names[i] != aliases[i]) {
				throw BinderException("Cannot query Iceberg view '%s': its query selects *, and column %d of the query "
				                      "is '%s' where the view stores '%s'. Recreate the view, listing its columns "
				                      "instead of *",
				                      name.GetIdentifierName(), i + 1, query_names[i].GetIdentifierName(),
				                      aliases[i].GetIdentifierName());
			}
		}
	}
	ViewCatalogEntry::UpdateBinding(types, names);
}

unique_ptr<CatalogEntry> IcebergViewEntry::Copy(ClientContext &context) const {
	auto info = GetInfo();
	return make_uniq<IcebergViewEntry>(catalog, schema, info->Cast<CreateViewInfo>(), context);
}

} // namespace duckdb
