#include "execution/operator/iceberg_copy_on_write_delete.hpp"

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/optimizer/join_filter_pushdown_optimizer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "execution/operator/iceberg_file_rewrite.hpp"

namespace duckdb {

namespace {

class CopyOnWriteDeleteRewrite : public IcebergFileRewrite {
public:
	CopyOnWriteDeleteRewrite(PhysicalPlan &physical_plan, IcebergTable &table,
	                         IcebergCopyOnWriteDeleteCollector &collector, unique_ptr<LogicalOperator> logical_plan)
	    : IcebergFileRewrite(physical_plan, {LogicalType::BIGINT}, 1, IcebergWriteOperation::COPY_ON_WRITE_DELETE),
	      table(table), collector(collector), logical_plan(std::move(logical_plan)) {
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		return make_uniq<IcebergFileRewriteGlobalState>(context, table);
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(collector.GetDeletes().deleted_count)));
		chunk.SetChildCardinality(1);
		return SourceResultType::FINISHED;
	}

protected:
	IcebergFileRewriteInput GetRewriteInput(const IcebergFileRewriteGlobalState &state) const override {
		//! Replacement rows come from this scan, so retain OVERWRITE's non-retryable policy.
		//! Without replacement files, DELETE only invalidates stable paths. Its manifest identities
		//! are rebuilt for each commit attempt, including those of files inserted in this transaction.
		IcebergFileRewriteInput result(state.added_data_files ? IcebergSnapshotOperationType::OVERWRITE
		                                                      : IcebergSnapshotOperationType::DELETE);
		for (auto &file : collector.GetDeletes().files) {
			result.replaced_files.emplace_back(file);
		}
		return result;
	}

private:
	IcebergTable &table;
	IcebergCopyOnWriteDeleteCollector &collector;
	//! Owns the logical plan the rewritten rows were planned from
	unique_ptr<LogicalOperator> logical_plan;
};

//! The body of the `deletee` CTE: the DELETE's own scan, reduced to the distinct (filename, file_row_number) pairs it
//! matches. DELETE ... USING can match the same target row more than once, so the DISTINCT keeps the count exact.
//! The DELETE's plan is already optimized and resolved, so it is planned as is rather than as a child of this tree.
class LogicalCopyOnWriteDeleteMatches : public LogicalExtensionOperator {
public:
	LogicalCopyOnWriteDeleteMatches(LogicalOperator &match_plan, vector<idx_t> row_id_indexes,
	                                vector<LogicalType> row_id_types)
	    : match_plan(match_plan), row_id_indexes(std::move(row_id_indexes)), row_id_types(std::move(row_id_types)) {
		types = this->row_id_types;
	}

	LogicalOperator &match_plan;
	//! The filename and file_row_number columns of match_plan, and their types
	vector<idx_t> row_id_indexes;
	vector<LogicalType> row_id_types;
	optional_ptr<IcebergCopyOnWriteDeleteCollector> collector;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &match = planner.CreatePlan(match_plan);

		vector<unique_ptr<Expression>> select_list;
		for (idx_t i = 0; i < row_id_indexes.size(); i++) {
			select_list.push_back(make_uniq<BoundReferenceExpression>(types[i], row_id_indexes[i]));
		}
		auto &projection = planner.Make<PhysicalProjection>(types, std::move(select_list), match.estimated_cardinality);
		projection.children.push_back(match);

		vector<unique_ptr<Expression>> groups;
		for (idx_t i = 0; i < types.size(); i++) {
			groups.push_back(make_uniq<BoundReferenceExpression>(types[i], i));
		}
		auto &distinct = planner.Make<PhysicalHashAggregate>(context, types, vector<unique_ptr<Expression>>(),
		                                                     std::move(groups), projection.estimated_cardinality);
		distinct.children.push_back(projection);

		auto &result = planner.Make<IcebergCopyOnWriteDeleteCollector>(types, distinct.estimated_cardinality);
		result.children.push_back(distinct);
		collector = result.Cast<IcebergCopyOnWriteDeleteCollector>();
		return result;
	}

	string GetExtensionName() const override {
		return "iceberg_copy_on_write_delete_matches";
	}

protected:
	void ResolveTypes() override {
		types = row_id_types;
	}
};

class CollectorGlobalState : public GlobalOperatorState {
public:
	mutex lock;
	IcebergCopyOnWriteDeletes deletes;
};

class CollectorLocalState : public OperatorState {
public:
	//! The files this thread has already recorded, so the global lock is only taken for new ones
	unordered_set<string> seen_files;
	vector<string> new_files;
};

//! Bind `SELECT * FROM <table>`, then add the filename and file_row_number virtual columns to its scan. They are
//! added by column id, not by name, so a table column that happens to be called `filename` cannot shadow them.
unique_ptr<LogicalOperator> BindKeepScan(Binder &binder, IcebergTableSchemaVersion &table) {
	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(make_uniq<StarExpression>());
	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->SetQualifiedName(table.ParentCatalog().GetName(), table.ParentSchema().name, table.name);
	select->from_table = std::move(table_ref);

	auto bound = binder.Bind(*select);
	auto plan = std::move(bound.plan);
	if (plan->type != LogicalOperatorType::LOGICAL_PROJECTION || plan->children.size() != 1 ||
	    plan->children[0]->type != LogicalOperatorType::LOGICAL_GET) {
		throw InternalException("Copy-on-write DELETE: expected the table scan to bind as a projection over a scan");
	}
	auto &projection = plan->Cast<LogicalProjection>();
	auto &get = plan->children[0]->Cast<LogicalGet>();
	for (auto virtual_column :
	     {MultiFileReader::COLUMN_IDENTIFIER_FILENAME, MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER}) {
		auto entry = get.virtual_columns.find(virtual_column);
		if (entry == get.virtual_columns.end()) {
			throw InternalException("Copy-on-write DELETE: the Iceberg scan does not expose virtual column %d",
			                        static_cast<int64_t>(virtual_column));
		}
		auto &column_ids = get.GetMutableColumnIds();
		column_ids.emplace_back(virtual_column);
		ColumnBinding binding(get.table_index, ProjectionIndex(column_ids.size() - 1));
		projection.expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(entry->second.name, entry->second.type, binding));
	}
	return plan;
}

void MaxTableIndex(const LogicalOperator &op, idx_t &result) {
	for (auto &index : op.GetTableIndex()) {
		result = MaxValue(result, index.index);
	}
	for (auto &child : op.children) {
		MaxTableIndex(*child, result);
	}
}

unique_ptr<Expression> ColumnRef(const LogicalType &type, TableIndex table_index, idx_t column_index) {
	return make_uniq<BoundColumnRefExpression>(type, ColumnBinding(table_index, ProjectionIndex(column_index)));
}

} // namespace

PhysicalOperator &IcebergCopyOnWriteDelete::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                       LogicalDelete &op, IcebergTableSchemaVersion &table) {
	auto &metadata = table.table_info.GetMetadata();
	if (metadata.iceberg_version >= 3) {
		//! A V3 rewrite has to carry each kept row's _row_id and _last_updated_sequence_number into the new files
		//! Pending rows may not have their final lineage yet, so carrying scan-time values is not sufficient.
		throw NotImplementedException("Copy-on-write DELETE on Iceberg V%d tables", metadata.iceberg_version);
	}

	//! deletee: the DELETE's own scan emits the filename and file_row_number of every matching row
	auto &match_plan = *op.children[0];
	vector<idx_t> row_id_indexes;
	vector<LogicalType> row_id_types;
	for (idx_t i = 0; i < 2; i++) {
		auto &ref = op.expressions[i]->Cast<BoundReferenceExpression>();
		row_id_indexes.push_back(ref.Index());
		row_id_types.push_back(ref.GetReturnType());
	}
	auto matches = make_uniq<LogicalCopyOnWriteDeleteMatches>(match_plan, std::move(row_id_indexes), row_id_types);
	auto &matches_ref = *matches;

	//! keep: the whole table again, with its filename and file_row_number as the last two columns
	auto binder = Binder::CreateBinder(context);
	auto keep = BindKeepScan(*binder, table);
	auto keep_index = keep->Cast<LogicalProjection>().table_index;
	auto column_count = keep->Cast<LogicalProjection>().expressions.size() - 2;
	vector<LogicalType> column_types;
	for (auto &expr : keep->Cast<LogicalProjection>().expressions) {
		column_types.push_back(expr->GetReturnType());
	}

	//! The CTE index has to be distinct from any CTE in the DELETE's plan: they share the physical planner
	idx_t max_index = 0;
	MaxTableIndex(match_plan, max_index);
	MaxTableIndex(*keep, max_index);
	TableIndex cte_index(MaxValue(max_index + 1, binder->GenerateTableIndex().index));
	auto cte_ref = [&]() {
		return make_uniq<LogicalCTERef>(binder->GenerateTableIndex(), cte_index, row_id_types,
		                                vector<Identifier> {"filename", "file_row_number"});
	};

	//! touched: SELECT DISTINCT filename FROM deletee
	auto touched_ref = cte_ref();
	auto touched_ref_index = touched_ref->table_index;
	auto touched_projection_index = binder->GenerateTableIndex();
	vector<unique_ptr<Expression>> touched_list;
	touched_list.push_back(ColumnRef(row_id_types[0], touched_ref_index, 0));
	auto touched_projection = make_uniq<LogicalProjection>(touched_projection_index, std::move(touched_list));
	touched_projection->AddChild(std::move(touched_ref));
	vector<unique_ptr<Expression>> distinct_targets;
	distinct_targets.push_back(ColumnRef(row_id_types[0], touched_projection_index, 0));
	auto touched = make_uniq<LogicalDistinct>(std::move(distinct_targets), DistinctType::DISTINCT);
	touched->AddChild(std::move(touched_projection));

	//! keep SEMI JOIN touched ON keep.filename = touched.filename
	auto semi_join = make_uniq<LogicalComparisonJoin>(JoinType::SEMI);
	auto &semi_join_ref = *semi_join;
	semi_join->conditions.emplace_back(ColumnRef(row_id_types[0], keep_index, column_count),
	                                   ColumnRef(row_id_types[0], touched_projection_index, 0),
	                                   ExpressionType::COMPARE_EQUAL);
	semi_join->AddChild(std::move(keep));
	semi_join->AddChild(std::move(touched));

	//! ANTI JOIN deletee ON keep.filename = deletee.filename AND keep.file_row_number = deletee.file_row_number
	auto deletee_ref = cte_ref();
	auto deletee_index = deletee_ref->table_index;
	auto anti_join = make_uniq<LogicalComparisonJoin>(JoinType::ANTI);
	for (idx_t i = 0; i < 2; i++) {
		anti_join->conditions.emplace_back(ColumnRef(row_id_types[i], keep_index, column_count + i),
		                                   ColumnRef(row_id_types[i], deletee_index, i), ExpressionType::COMPARE_EQUAL);
	}
	anti_join->AddChild(std::move(semi_join));
	anti_join->AddChild(std::move(deletee_ref));

	//! SELECT keep.*
	vector<unique_ptr<Expression>> keep_list;
	for (idx_t i = 0; i < column_count; i++) {
		keep_list.push_back(ColumnRef(column_types[i], keep_index, i));
	}
	unique_ptr<LogicalOperator> rows = make_uniq<LogicalProjection>(binder->GenerateTableIndex(), std::move(keep_list));
	rows->AddChild(std::move(anti_join));

	//! The semi join restricts the keep scan to the touched files at runtime: its build side becomes a filter on the
	//! constant `filename` column, which the scan evaluates once per file, skipping every other file unread.
	Optimizer optimizer(*binder, context);
	JoinFilterPushdownOptimizer join_filter_pushdown(optimizer);
	join_filter_pushdown.VisitOperator(*rows);
	if (semi_join_ref.filter_pushdown) {
		//! The DELETE's predicate usually selects few rows, so the touched files are worth filtering on
		semi_join_ref.filter_pushdown->build_side_has_filter = true;
	}

	//! The DELETE's plan is resolved already; only the new operators above the CTE body need it.
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*rows);

	unique_ptr<LogicalOperator> plan = make_uniq<LogicalMaterializedCTE>(
	    "deletee", cte_index, 2, std::move(matches), std::move(rows), CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	plan->ResolveOperatorTypes();
	auto &rows_plan = planner.CreatePlan(*plan);
	D_ASSERT(rows_plan.GetTypes().size() == column_count);
	if (!matches_ref.collector) {
		throw InternalException("Copy-on-write DELETE: the deleted rows are not collected");
	}

	//! Use the same replacement-file writer and snapshot staging as maintenance rewrites.
	auto &rewrite = planner.Make<CopyOnWriteDeleteRewrite>(table.table_info, *matches_ref.collector, std::move(plan))
	                    .Cast<CopyOnWriteDeleteRewrite>();
	rewrite.PlanCopy(context, planner, metadata, rows_plan);
	return rewrite;
}

//===--------------------------------------------------------------------===//
// Collector
//===--------------------------------------------------------------------===//
IcebergCopyOnWriteDeleteCollector::IcebergCopyOnWriteDeleteCollector(PhysicalPlan &physical_plan,
                                                                     vector<LogicalType> types,
                                                                     idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality) {
}

unique_ptr<GlobalOperatorState>
IcebergCopyOnWriteDeleteCollector::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<CollectorGlobalState>();
}

unique_ptr<OperatorState> IcebergCopyOnWriteDeleteCollector::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<CollectorLocalState>();
}

OperatorResultType IcebergCopyOnWriteDeleteCollector::Execute(ExecutionContext &context, DataChunk &input,
                                                              DataChunk &chunk, GlobalOperatorState &gstate_p,
                                                              OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<CollectorGlobalState>();
	auto &state = state_p.Cast<CollectorLocalState>();

	UnifiedVectorFormat file_name_data;
	input.data[0].ToUnifiedFormat(file_name_data);
	auto file_names = UnifiedVectorFormat::GetData<string_t>(file_name_data);
	for (idx_t i = 0; i < input.size(); i++) {
		auto file_idx = file_name_data.sel->get_index(i);
		if (!file_name_data.validity.RowIsValid(file_idx)) {
			throw InternalException("Filename cannot be NULL!");
		}
		auto file_name = file_names[file_idx].GetString();
		if (state.seen_files.insert(file_name).second) {
			state.new_files.push_back(std::move(file_name));
		}
	}

	{
		lock_guard<mutex> guard(gstate.lock);
		//! The CTE body is DISTINCT, so every row is a distinct deleted position
		gstate.deletes.deleted_count += input.size();
		for (auto &file : state.new_files) {
			gstate.deletes.files.insert(std::move(file));
		}
	}
	state.new_files.clear();

	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

const IcebergCopyOnWriteDeletes &IcebergCopyOnWriteDeleteCollector::GetDeletes() const {
	if (!op_state) {
		throw InternalException("Copy-on-write DELETE: the deleted rows were read before they were collected");
	}
	return op_state->Cast<CollectorGlobalState>().deletes;
}

string IcebergCopyOnWriteDeleteCollector::GetName() const {
	return "ICEBERG_COPY_ON_WRITE_DELETED_ROWS";
}

} // namespace duckdb
