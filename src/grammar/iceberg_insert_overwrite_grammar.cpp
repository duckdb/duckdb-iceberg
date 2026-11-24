#include "grammar/iceberg_insert_overwrite_grammar.hpp"

#include "execution/operator/iceberg_insert.hpp"
#include "execution/operator/iceberg_insert_overwrite.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "common/iceberg_utils.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/grammar_change.hpp"
#include "duckdb/parser/grammar_extension.hpp"
#include "duckdb/parser/peg/ast/insert_values.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/planner_extension.hpp"

namespace duckdb {

namespace {

//! An entry of the PARTITION clause: 'column = value' (static) or 'column'
struct IcebergOverwritePartitionEntry {
	string column;
	//! Null for an entry without a value
	unique_ptr<ParsedExpression> value;

	IcebergOverwritePartitionEntry Copy() const {
		IcebergOverwritePartitionEntry result;
		result.column = column;
		result.value = value ? value->Copy() : nullptr;
		return result;
	}
};

//! The PARTITION clause after its values were evaluated, as the pre-bind hook hands it to the insert's plan
struct IcebergBoundOverwrite {
	//! Every column the clause names
	vector<string> partition_columns;
	//! The entries that have a value, cast to the type of their column
	vector<std::pair<string, Value>> static_values;
	//! A value is a parameter that has no value yet: the statement is only being prepared
	bool missing_static_values = false;
};

//! The insert node of an INSERT OVERWRITE. The core binder binds it like any insert node; the planner hooks below carry
//! what it replaces to the insert operator. The overwrite is part of the node, not of the statement around it: the
//! node is what the core moves, copies, prints and serializes
class IcebergInsertOverwriteNode : public InsertQueryNode {
public:
	vector<IcebergOverwritePartitionEntry> partition;

public:
	//! The copy the base class makes: an insert node without the overwrite
	static unique_ptr<InsertQueryNode> CopyInsert(const InsertQueryNode &node) {
		return unique_ptr_cast<QueryNode, InsertQueryNode>(node.InsertQueryNode::Copy());
	}

	unique_ptr<QueryNode> Copy() const override {
		auto result = make_uniq<IcebergInsertOverwriteNode>();
		result->TakeInsert(*CopyInsert(*this));
		//! TakeInsert lists the fields of an insert node by hand
		D_ASSERT(result->InsertQueryNode::Equals(this));
		for (auto &entry : partition) {
			result->partition.push_back(entry.Copy());
		}
		return std::move(result);
	}

	string ToString() const override {
		//! The text of the INSERT, with INTO replaced and the PARTITION clause after the table name
		auto insert = InsertQueryNode::ToString();
		const string into = cte_map.ToString() + "INSERT INTO " + qualified_name.ToString();
		if (insert.compare(0, into.size(), into) != 0) {
			throw InternalException("Unexpected INSERT text for INSERT OVERWRITE: %s", insert);
		}
		auto result = cte_map.ToString() + "INSERT OVERWRITE " + qualified_name.ToString();
		if (!partition.empty()) {
			result += " PARTITION (";
			for (idx_t i = 0; i < partition.size(); i++) {
				if (i > 0) {
					result += ", ";
				}
				result += StringUtil::Format("%s", SQLIdentifier(partition[i].column));
				if (partition[i].value) {
					result += " = " + partition[i].value->ToString();
				}
			}
			result += ')';
		}
		return result + insert.substr(into.size());
	}

	bool Equals(const QueryNode *other_p) const override {
		//! The node class is defined in this extension, so the cast does not cross a linking boundary
		auto other = dynamic_cast<const IcebergInsertOverwriteNode *>(other_p);
		if (!other || !InsertQueryNode::Equals(other_p) || partition.size() != other->partition.size()) {
			return false;
		}
		for (idx_t i = 0; i < partition.size(); i++) {
			if (partition[i].column != other->partition[i].column ||
			    !ParsedExpression::Equals(partition[i].value, other->partition[i].value)) {
				return false;
			}
		}
		return true;
	}

	void Serialize(Serializer &serializer) const override {
		//! A deserialized node is a plain insert node: it would run as an INSERT INTO
		throw SerializationException("INSERT OVERWRITE cannot be serialized");
	}

private:
	//! Moves the fields of an insert node into this one
	void TakeInsert(InsertQueryNode &insert) {
		select_statement = std::move(insert.select_statement);
		columns = std::move(insert.columns);
		qualified_name = std::move(insert.qualified_name);
		returning_list = std::move(insert.returning_list);
		on_conflict_info = std::move(insert.on_conflict_info);
		table_ref = std::move(insert.table_ref);
		default_values = insert.default_values;
		column_order = insert.column_order;
		modifiers = std::move(insert.modifiers);
		cte_map = std::move(insert.cte_map);
	}
};

//! The insert node of an INSERT OVERWRITE statement, if it is one
optional_ptr<IcebergInsertOverwriteNode> FindOverwriteNode(SQLStatement &statement) {
	if (statement.type == StatementType::EXPLAIN_STATEMENT) {
		//! EXPLAIN binds its statement with the same binder, without another round of planner hooks
		return FindOverwriteNode(*statement.Cast<ExplainStatement>().stmt);
	}
	if (statement.type != StatementType::INSERT_STATEMENT) {
		return nullptr;
	}
	return dynamic_cast<IcebergInsertOverwriteNode *>(statement.Cast<InsertStatement>().node.get());
}

//! IcebergOverwritePartitionClause <- 'PARTITION' Parens(List(IcebergOverwritePartitionEntry))
//! IcebergOverwritePartitionEntry <- ColId IcebergOverwritePartitionValue?
//! IcebergOverwritePartitionValue <- '=' Expression
vector<IcebergOverwritePartitionEntry> TransformPartitionClause(PEGTransformer &transformer,
                                                                ParseResult &parse_result) {
	auto &clause = parse_result.Cast<ListParseResult>();
	auto entries = PEGTransformerFactory::ExtractParseResultsFromList(
	    PEGTransformerFactory::ExtractResultFromParens(clause.GetChild(1)));
	vector<IcebergOverwritePartitionEntry> result;
	case_insensitive_set_t names;
	for (auto &entry_result : entries) {
		auto &entry_list = entry_result.get().Cast<ListParseResult>();
		IcebergOverwritePartitionEntry entry;
		entry.column = transformer.Transform<Identifier>(entry_list.GetChild(0)).GetIdentifierName();
		if (!names.insert(entry.column).second) {
			throw ParserException("PARTITION clause names column \"%s\" more than once", entry.column);
		}
		auto &value = entry_list.Child<OptionalParseResult>(1);
		if (value.HasResult()) {
			auto &value_list = value.GetResult().Cast<ListParseResult>();
			entry.value = transformer.Transform<unique_ptr<ParsedExpression>>(value_list.GetChild(1));
		}
		result.push_back(std::move(entry));
	}
	return result;
}

//! IcebergInsertOverwrite <- WithClause? 'INSERT' 'OVERWRITE' 'TABLE'? BaseTableName IcebergOverwritePartitionClause?
//! ByNameOrPosition? InsertColumnList? InsertValues
arena_ptr<TransformResultValue> FinalizeInsertOverwrite(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto overwrite = make_uniq<IcebergInsertOverwriteNode>();
	auto &node = *overwrite;

	transformer.TransformOptional(list, 0, node.cte_map);
	auto target = transformer.Transform<unique_ptr<BaseTableRef>>(list.GetChild(4));
	node.qualified_name = target->GetQualifiedName();
	auto &partition_clause = list.Child<OptionalParseResult>(5);
	if (partition_clause.HasResult()) {
		overwrite->partition = TransformPartitionClause(transformer, partition_clause.GetResult());
	}
	auto column_order = InsertColumnOrder::INSERT_BY_POSITION;
	transformer.TransformOptional(list, 6, column_order);
	node.column_order = column_order;
	vector<string> columns;
	transformer.TransformOptional(list, 7, columns);
	node.columns = StringsToIdentifiers(columns);
	auto values = transformer.Transform<InsertValues>(list.GetChild(8));
	if (values.default_values) {
		throw ParserException("INSERT OVERWRITE needs a query, DEFAULT VALUES is not supported");
	}
	node.select_statement = std::move(values.select_statement);

	auto result = make_uniq<InsertStatement>();
	result->node = std::move(overwrite);
	unique_ptr<SQLStatement> statement = std::move(result);
	return transformer.MakeResult<unique_ptr<SQLStatement>>(std::move(statement));
}

arena_ptr<TransformProcess> StartInsertOverwriteTransform(PEGTransformer &transformer, ParseResult &parse_result) {
	return transformer.MakeProcess<FinalizeTransformProcess>(transformer, parse_result, FinalizeInsertOverwrite);
}

//! CTEDMLBody <- Parens(Statement)
//! The core transform keeps only the insert node of a statement: an INSERT OVERWRITE would run as an INSERT INTO
arena_ptr<TransformResultValue> FinalizeCTEDMLBody(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &body = parse_result.Cast<ListParseResult>();
	auto statement = transformer.Transform<unique_ptr<SQLStatement>>(
	    PEGTransformerFactory::ExtractResultFromParens(body.GetChild(0)));
	if (FindOverwriteNode(*statement)) {
		throw ParserException("INSERT OVERWRITE cannot be used in a WITH clause");
	}
	auto result = PEGTransformerFactory::TransformCTEDMLBody(transformer, std::move(statement));
	return transformer.MakeResult<unique_ptr<TableRef>>(std::move(result));
}

arena_ptr<TransformProcess> StartCTEDMLBodyTransform(PEGTransformer &transformer, ParseResult &parse_result) {
	return transformer.MakeProcess<FinalizeTransformProcess>(transformer, parse_result, FinalizeCTEDMLBody);
}

class IcebergInsertOverwriteGrammar final : public GrammarExtension {
public:
	IcebergInsertOverwriteGrammar()
	    : GrammarExtension("iceberg_insert_overwrite", "INSERT OVERWRITE for Iceberg tables") {
	}

	vector<GrammarChange> GetChanges() const override {
		vector<GrammarChange> changes;
		changes.push_back(
		    GrammarChange::AddRule("IcebergInsertOverwrite <- WithClause? 'INSERT' 'OVERWRITE' 'TABLE'? BaseTableName "
		                           "IcebergOverwritePartitionClause? ByNameOrPosition? InsertColumnList? InsertValues",
		                           StartInsertOverwriteTransform));
		changes.push_back(GrammarChange::AddRule(
		    "IcebergOverwritePartitionClause <- 'PARTITION' Parens(List(IcebergOverwritePartitionEntry))"));
		changes.push_back(
		    GrammarChange::AddRule("IcebergOverwritePartitionEntry <- ColId IcebergOverwritePartitionValue?"));
		changes.push_back(GrammarChange::AddRule("IcebergOverwritePartitionValue <- '=' Expression"));
		//! Tried before the built-in INSERT, which needs INTO
		changes.push_back(GrammarChange::PrependChoice("Statement", "IcebergInsertOverwrite"));
		//! EXPLAIN lists the statements it takes in a rule of its own
		changes.push_back(GrammarChange::PrependChoice("ExplainableStatements", "IcebergInsertOverwrite"));
		//! A WITH clause takes any statement as its body and must not take this one
		changes.push_back(GrammarChange::SetTransformProcess("CTEDMLBody", StartCTEDMLBodyTransform));
		return changes;
	}
};

//! The insert of an INSERT OVERWRITE: plans its child, the bound INSERT, and tells its operator what it replaces
struct IcebergLogicalInsertOverwrite : public LogicalExtensionOperator {
	explicit IcebergLogicalInsertOverwrite(IcebergBoundOverwrite overwrite_p) : overwrite(std::move(overwrite_p)) {
	}

	IcebergBoundOverwrite overwrite;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &insert = planner.CreatePlan(*children[0]).Cast<IcebergInsert>();
		//! Checked on the table as the transaction sees it: an earlier statement can have changed its partition spec
		auto &table = insert.table->Cast<IcebergTableSchemaVersion>();
		auto &metadata = table.table_info.GetMetadata();
		auto table_name = table.name.GetIdentifierName();
		IcebergOverwriteSpec spec;
		for (auto &column : overwrite.partition_columns) {
			IcebergInsertOverwrite::ResolvePartitionColumn(metadata, table_name, column);
		}
		for (auto &static_value : overwrite.static_values) {
			IcebergOverwriteStaticValue resolved;
			resolved.column_index =
			    IcebergInsertOverwrite::ResolvePartitionColumn(metadata, table_name, static_value.first);
			resolved.value = static_value.second;
			spec.static_values.push_back(std::move(resolved));
		}
		spec.missing_static_values = overwrite.missing_static_values;
		IcebergInsertOverwrite::PlanStaticOverwrite(context, table.table_info, spec);
		insert.overwrite = std::move(spec);
		return insert;
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

	string GetName() const override {
		return "ICEBERG_INSERT_OVERWRITE";
	}

	string GetExtensionName() const override {
		return "iceberg";
	}

	bool SupportSerialization() const override {
		return false;
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};

//! Carries the overwrite of the statement a binder is about to bind from the pre-bind hook to the post-bind hook
struct IcebergOverwritePlannerState : public ClientContextState {
	static constexpr const char *NAME = "iceberg_insert_overwrite";

	reference_map_t<const Binder, IcebergBoundOverwrite> pending;

	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override {
		//! A bind that failed never reached the post-bind hook
		pending.clear();
	}
};

void VerifyIcebergTable(TableCatalogEntry &table) {
	if (!IcebergUtils::IsIcebergCatalog(table.ParentCatalog())) {
		throw BinderException("INSERT OVERWRITE is only supported for Iceberg tables");
	}
}

//! Evaluates the static PARTITION values and adds them to the query of 'node', which supplies the other columns
void BindStaticValues(PlannerExtensionInput &input, IcebergInsertOverwriteNode &node, IcebergBoundOverwrite &result) {
	//! The table, found as the binder will find it. A missing table is the binder's error to report
	auto table_name = input.binder.BindTableName(node.qualified_name);
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, table_name);
	auto table_entry = input.binder.EntryRetriever().GetEntry(table_lookup, OnEntryNotFound::RETURN_NULL);
	if (!table_entry || table_entry->type != CatalogType::TABLE_ENTRY) {
		return;
	}
	auto &table = table_entry->Cast<TableCatalogEntry>();
	VerifyIcebergTable(table);
	auto &table_columns = table.GetColumns();

	//! A value can depend on when the statement runs (a parameter, the current date): bind again for every run
	input.binder.SetAlwaysRequireRebind();

	case_insensitive_set_t static_columns;
	vector<Identifier> static_column_names;
	vector<unique_ptr<ParsedExpression>> static_expressions;
	for (auto &entry : node.partition) {
		if (!entry.value) {
			continue;
		}
		if (!table_columns.ColumnExists(Identifier(entry.column))) {
			throw BinderException(
			    "PARTITION clause names \"%s\", which is not an identity partition column of table \"%s\"",
			    entry.column, table.name.GetIdentifierName());
		}
		auto &column = table_columns.GetColumn(Identifier(entry.column));
		auto column_name = column.Name().GetIdentifierName();
		for (auto &listed : node.columns) {
			if (StringUtil::CIEquals(listed.GetIdentifierName(), entry.column)) {
				throw BinderException("Static partition column \"%s\" is also specified in the column list",
				                      entry.column);
			}
		}

		auto expression = entry.value->Copy();
		ConstantBinder value_binder(input.binder, input.context, "PARTITION values");
		auto bound = value_binder.Bind(expression);
		unique_ptr<ParsedExpression> written_value;
		if (bound->HasParameter()) {
			//! PREPARE: the value comes with EXECUTE, which binds the statement again
			result.missing_static_values = true;
			written_value = entry.value->Copy();
		} else {
			if (!bound->IsFoldable()) {
				throw BinderException("PARTITION value %s of column \"%s\" is not a constant", entry.value->ToString(),
				                      entry.column);
			}
			auto value = IcebergInsertOverwrite::CastPartitionValue(
			    input.context, ExpressionExecutor::EvaluateScalar(input.context, *bound), column.Type(), column_name);
			written_value = ConstantExpression::FromValue(value);
			result.static_values.emplace_back(column_name, std::move(value));
		}
		//! Named after the column, for BY NAME
		written_value->SetAlias(Identifier(column_name));
		static_columns.insert(column_name);
		static_column_names.emplace_back(column_name);
		static_expressions.push_back(std::move(written_value));
	}

	if (node.column_order == InsertColumnOrder::INSERT_BY_POSITION) {
		if (node.columns.empty()) {
			//! The query supplies the columns that have no static value, in table order
			for (auto &column : table_columns.Physical()) {
				if (!static_columns.count(column.Name().GetIdentifierName())) {
					node.columns.push_back(column.Name());
				}
			}
		}
		for (auto &name : static_column_names) {
			node.columns.push_back(name);
		}
		auto values_list = node.GetValuesList();
		if (values_list) {
			//! A VALUES list stays the source of the INSERT, which binds DEFAULT and the type of each column for it
			for (auto &row : values_list->values) {
				for (auto &expression : static_expressions) {
					row.push_back(expression->Copy());
				}
			}
			return;
		}
	}
	if (node.GetValuesList()) {
		//! BY NAME with a VALUES list: left as written, the binder refuses it
		return;
	}
	//! SELECT *, <static values> FROM (<query>)
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	for (auto &expression : static_expressions) {
		select_node->select_list.push_back(std::move(expression));
	}
	select_node->from_table = make_uniq<SubqueryRef>(std::move(node.select_statement));
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	node.select_statement = std::move(select_statement);
}

//! A data-modifying statement in the WITH clause would leave more than one insert in the plan
void VerifyNoDataModifyingCTE(const InsertQueryNode &node) {
	for (auto &cte : node.cte_map.map) {
		switch (cte.second->query_node->type) {
		case QueryNodeType::INSERT_QUERY_NODE:
		case QueryNodeType::UPDATE_QUERY_NODE:
		case QueryNodeType::DELETE_QUERY_NODE:
		case QueryNodeType::MERGE_QUERY_NODE:
		case QueryNodeType::COPY_QUERY_NODE:
			throw BinderException(
			    "INSERT OVERWRITE cannot be combined with a data-modifying statement in its WITH clause");
		default:
			break;
		}
	}
}

void PreBindInsertOverwrite(PlannerExtensionInput &input, SQLStatement &statement) {
	auto overwrite = FindOverwriteNode(statement);
	if (!overwrite) {
		//! Whatever an earlier bind left behind for a binder at this address does not belong to this statement
		auto state =
		    input.context.registered_state->Get<IcebergOverwritePlannerState>(IcebergOverwritePlannerState::NAME);
		if (state) {
			state->pending.erase(input.binder);
		}
		return;
	}
	VerifyNoDataModifyingCTE(*overwrite);

	IcebergBoundOverwrite bound;
	bool has_static_values = false;
	for (auto &entry : overwrite->partition) {
		bound.partition_columns.push_back(entry.column);
		has_static_values |= entry.value != nullptr;
	}
	if (has_static_values) {
		BindStaticValues(input, *overwrite, bound);
	}
	auto state =
	    input.context.registered_state->GetOrCreate<IcebergOverwritePlannerState>(IcebergOverwritePlannerState::NAME);
	state->pending[input.binder] = std::move(bound);
}

void FindInserts(unique_ptr<LogicalOperator> &op, vector<reference<unique_ptr<LogicalOperator>>> &inserts) {
	if (op->type == LogicalOperatorType::LOGICAL_INSERT) {
		inserts.push_back(op);
	}
	for (auto &child : op->children) {
		FindInserts(child, inserts);
	}
}

void PostBindInsertOverwrite(PlannerExtensionInput &input, BoundStatement &statement) {
	auto state = input.context.registered_state->Get<IcebergOverwritePlannerState>(IcebergOverwritePlannerState::NAME);
	if (!state) {
		return;
	}
	auto entry = state->pending.find(input.binder);
	if (entry == state->pending.end()) {
		return;
	}
	auto overwrite = std::move(entry->second);
	state->pending.erase(entry);

	vector<reference<unique_ptr<LogicalOperator>>> inserts;
	if (statement.plan) {
		FindInserts(statement.plan, inserts);
	}
	//! A trigger on a table of another catalog adds inserts of its own
	for (auto &bound_insert : inserts) {
		VerifyIcebergTable(bound_insert.get()->Cast<LogicalInsert>().table);
	}
	//! An Iceberg table has no triggers and the statement no other data-modifying part: the insert is its own
	if (inserts.size() != 1) {
		throw InternalException("INSERT OVERWRITE was bound to %d inserts", inserts.size());
	}
	auto &insert = inserts[0].get();
	auto wrapper = make_uniq<IcebergLogicalInsertOverwrite>(std::move(overwrite));
	wrapper->children.push_back(std::move(insert));
	insert = std::move(wrapper);
}

} // namespace

void RegisterIcebergInsertOverwrite(DatabaseInstance &db) {
	GrammarExtension::Register(db, make_shared_ptr<IcebergInsertOverwriteGrammar>());

	PlannerExtension planner_extension;
	planner_extension.pre_bind_function = PreBindInsertOverwrite;
	planner_extension.post_bind_function = PostBindInsertOverwrite;
	PlannerExtension::Register(DBConfig::GetConfig(db), std::move(planner_extension));
}

} // namespace duckdb
