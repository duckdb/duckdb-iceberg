#include "catalog/rest/iceberg_table_prefetch.hpp"

#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

IcebergTablePrefetch::IcebergTablePrefetch(ClientContext &context, IcebergCatalog &catalog)
    : executor(context, catalog) {
}

IcebergTablePrefetch::~IcebergTablePrefetch() {
}

namespace {

//! Collects the base table references of a statement, including those in CTEs and in subquery expressions. Names that
//! refer to a CTE of the statement are skipped. Tables behind views aren't seen: the statement only names the view
class StatementTableCollector {
public:
	vector<QualifiedName> tables;

	void VisitStatement(SQLStatement &statement) {
		switch (statement.type) {
		case StatementType::SELECT_STATEMENT:
			VisitNode(statement.Cast<SelectStatement>().node);
			break;
		case StatementType::INSERT_STATEMENT:
			VisitNode(statement.Cast<InsertStatement>().node);
			break;
		case StatementType::UPDATE_STATEMENT:
			VisitNode(statement.Cast<UpdateStatement>().node);
			break;
		case StatementType::DELETE_STATEMENT:
			VisitNode(statement.Cast<DeleteStatement>().node);
			break;
		case StatementType::EXPLAIN_STATEMENT: {
			auto &explain = statement.Cast<ExplainStatement>();
			if (explain.stmt) {
				VisitStatement(*explain.stmt);
			}
			break;
		}
		case StatementType::CREATE_STATEMENT: {
			auto &create = statement.Cast<CreateStatement>();
			if (create.info && create.info->type == CatalogType::TABLE_ENTRY) {
				auto &query = create.info->Cast<CreateTableInfo>().query;
				if (query) {
					VisitNode(query->node);
				}
			}
			break;
		}
		default:
			break;
		}
	}

private:
	case_insensitive_set_t cte_names;

	template <class T>
	void VisitNode(unique_ptr<T> &node) {
		if (node) {
			VisitQueryNode(*node);
		}
	}

	void VisitQueryNode(QueryNode &node) {
		for (auto &cte : node.cte_map.map) {
			cte_names.insert(cte.first.GetIdentifierName());
			if (cte.second && cte.second->query_node) {
				VisitQueryNode(*cte.second->query_node);
			}
		}
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    node, [&](unique_ptr<ParsedExpression> &child) { VisitExpression(*child); },
		    [&](TableRef &ref) { VisitTableRef(ref); });
	}

	void VisitExpression(ParsedExpression &expr) {
		if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
			auto &subquery = expr.Cast<SubqueryExpression>().Subquery();
			if (subquery && subquery->node) {
				VisitQueryNode(*subquery->node);
			}
		}
		ParsedExpressionIterator::EnumerateChildren(expr, [&](ParsedExpression &child) { VisitExpression(child); });
	}

	void VisitTableRef(TableRef &ref) {
		if (ref.type != TableReferenceType::BASE_TABLE) {
			return;
		}
		auto &name = ref.Cast<BaseTableRef>().GetQualifiedName();
		if (name.Catalog().empty() && name.Schema().empty() && cte_names.count(name.Name().GetIdentifierName())) {
			return;
		}
		tables.push_back(name);
	}
};

//! The (catalog, schema) pairs a lookup of `name` tries, in search path order
vector<CatalogSearchEntry> GetCandidates(ClientContext &context, const QualifiedName &name) {
	auto &search_path = *ClientData::Get(context).catalog_search_path;
	vector<CatalogSearchEntry> candidates;
	if (!name.Catalog().empty()) {
		if (!name.Schema().empty()) {
			candidates.emplace_back(name.Catalog(), name.Schema());
		}
		return candidates;
	}
	if (name.Schema().empty()) {
		return search_path.Get();
	}
	for (auto &catalog_name : search_path.GetCatalogsForSchema(name.Schema())) {
		candidates.emplace_back(catalog_name, name.Schema());
	}
	if (candidates.empty()) {
		candidates.emplace_back(search_path.GetDefault().GetCatalog(), name.Schema());
	}
	//! `name.Schema()` may also be a catalog, e.g. `my_catalog.my_table` in the catalog's default schema
	auto catalog = Catalog::GetCatalogEntry(context, name.Schema());
	if (catalog && catalog->GetCatalogType() == "iceberg") {
		auto default_schema = catalog->Cast<IcebergCatalog>().GetDefaultSchema();
		if (default_schema) {
			candidates.emplace_back(name.Schema(), *default_schema);
		}
	}
	return candidates;
}

void PrefetchStatementTablesInternal(ClientContext &context, SQLStatement &statement) {
	StatementTableCollector collector;
	collector.VisitStatement(statement);
	if (collector.tables.empty()) {
		return;
	}
	reference_map_t<IcebergCatalog, vector<QualifiedName>> catalog_tables;
	for (auto &name : collector.tables) {
		// A lookup tries the candidates in order. Take the first one in a catalog that isn't a DuckDB catalog, whose
		// lookups are cheap: that is where a lookup that reaches a remote catalog looks first
		for (auto &candidate : GetCandidates(context, name)) {
			auto catalog = Catalog::GetCatalogEntry(context, candidate.GetCatalog());
			if (!catalog || catalog->IsDuckCatalog() || candidate.GetSchema().empty()) {
				continue;
			}
			if (catalog->GetCatalogType() == "iceberg") {
				auto &ic_catalog = catalog->Cast<IcebergCatalog>();
				catalog_tables[ic_catalog].push_back(
				    QualifiedName(candidate.GetCatalog(), candidate.GetSchema(), name.Name()));
			}
			break;
		}
	}
	for (auto &entry : catalog_tables) {
		IcebergTablePrefetch::PrefetchTables(context, entry.first.get(), entry.second);
	}
}

} // namespace

void IcebergTablePrefetch::PrefetchStatementTables(ClientContext &context, SQLStatement &statement) {
	try {
		PrefetchStatementTablesInternal(context, statement);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERRUPT) {
			throw;
		}
		//! Prefetching is an optimization: binding still resolves every table on its own
	}
}

void IcebergTablePrefetch::PrefetchTables(ClientContext &context, IcebergCatalog &catalog,
                                          const vector<QualifiedName> &tables) {
	auto &transaction = IcebergTransaction::Get(context, catalog);
	for (auto &name : tables) {
		auto &table_name = name.Name().GetIdentifierName();
		if (table_name.find_first_of("/\\") != string::npos) {
			//! A file path of a replacement scan, e.g. FROM 's3://bucket/file.parquet'. IcebergTableSet::GetEntry
			//! treats these names the same way
			continue;
		}
		auto schema_entry =
		    catalog.GetSchemas().GetEntry(context, name.Schema().GetIdentifierName(), OnEntryNotFound::RETURN_NULL);
		if (!schema_entry) {
			continue;
		}
		auto &schema = schema_entry->Cast<IcebergSchemaEntry>();
		auto table_key = IcebergTable::GetTableKey(catalog, schema.namespace_items, table_name);
		if (transaction.GetLatestTableState(table_key)) {
			continue;
		}
		auto existing = transaction.TryGetTablePrefetch();
		if (existing && existing->pending.count(table_key)) {
			continue;
		}
		if (catalog.attach_options.max_table_staleness_micros.IsValid() &&
		    catalog.table_request_cache.Get(context, table_key, [](const rest_api_objects::LoadTableResult &) {})) {
			continue;
		}
		auto &prefetch = transaction.GetTablePrefetch(context);
		PendingLoad load;
		load.publication = catalog.table_request_cache.BeginLoad(table_key);
		load.result = prefetch.executor.Schedule(IcebergLoadTableRequest(schema.namespace_items, table_name));
		prefetch.pending.emplace(std::move(table_key), std::move(load));
	}
}

bool IcebergTablePrefetch::TryTakeResult(const string &table_key, IcebergLoadTableResult &result,
                                         unique_ptr<LoadTableCachePublication> &publication) {
	auto entry = pending.find(table_key);
	if (entry == pending.end()) {
		return false;
	}
	auto load = std::move(entry->second);
	pending.erase(entry);
	try {
		result = executor.WaitAndTakeResult(*load.result);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERRUPT) {
			throw;
		}
		//! The caller requests the table itself
		return false;
	}
	publication = std::move(load.publication);
	return true;
}

} // namespace duckdb
