#include "execution/operator/iceberg_copy_on_write_delete.hpp"

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/optimizer/join_filter_pushdown_optimizer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "execution/operator/iceberg_delete.hpp"
#include "execution/operator/iceberg_file_rewrite.hpp"
#include "core/deletes/iceberg_delete_data.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "planning/iceberg_multi_file_reader.hpp"

namespace duckdb {

namespace {

//! SPEC: when removing a data file, writers must also remove any deletion vector that applies to it. Like a
//! merge-on-read DELETE replacing a vector, this also removes the file-scoped position delete files; partition-scoped
//! ones may still apply to other data files and stay.
void AddReplacedFile(IcebergFileRewriteInput &result, const string &file, const IcebergMultiFileList &match_file_list) {
	result.replaced_files.emplace_back(file);
	auto existing_deletes = match_file_list.GetExistingPositionalDeleteData(file);
	if (existing_deletes) {
		for (auto &delete_file : existing_deletes->source_files) {
			result.replaced_files.push_back(delete_file);
		}
	}
}

class CopyOnWriteUpdateRewrite : public IcebergFileRewrite {
public:
	CopyOnWriteUpdateRewrite(PhysicalPlan &physical_plan, IcebergTable &table,
	                         IcebergCopyOnWriteDeleteFilter &delete_filter)
	    : IcebergFileRewrite(physical_plan, {LogicalType::BIGINT}, 1, IcebergWriteOperation::COPY_ON_WRITE_DELETE),
	      table(table), delete_filter(delete_filter) {
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		return make_uniq<IcebergFileRewriteGlobalState>(context, table);
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(delete_filter.GetDeletes().deleted_count)));
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
		auto &rows_by_file = delete_filter.GetDeletes().rows_by_file;
		//! The binder takes filename and file_row_number from the target scan. If the optimizer
		//! eliminated that scan, the sink cannot have received a matching row.
		if (!rows_by_file.empty() && !delete_filter.match_file_list) {
			throw InternalException("Copy-on-write UPDATE: matched rows without an Iceberg scan of the matching rows");
		}
		for (auto &entry : rows_by_file) {
			AddReplacedFile(result, entry.first, *delete_filter.match_file_list);
		}
		return result;
	}

private:
	IcebergTable &table;
	IcebergCopyOnWriteDeleteFilter &delete_filter;
};

class CopyOnWriteDeleteRewrite : public IcebergFileRewrite {
public:
	CopyOnWriteDeleteRewrite(PhysicalPlan &physical_plan, IcebergTable &table,
	                         IcebergCopyOnWriteDeleteCollector &collector, IcebergCopyOnWriteRowCounter &touched_rows,
	                         IcebergCopyOnWriteRowCounter &kept_rows, unique_ptr<LogicalOperator> logical_plan)
	    : IcebergFileRewrite(physical_plan, {LogicalType::BIGINT}, 1, IcebergWriteOperation::COPY_ON_WRITE_DELETE),
	      table(table), collector(collector), touched_rows(touched_rows), kept_rows(kept_rows),
	      logical_plan(std::move(logical_plan)) {
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		return make_uniq<IcebergFileRewriteGlobalState>(context, table);
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(GetDeletedCount())));
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
		auto &files = collector.GetDeletes().files;
		//! The DELETE binder takes filename and file_row_number from the target scan. If the optimizer
		//! eliminated that scan, the collector cannot have received a matching row.
		if (!files.empty() && !collector.match_file_list) {
			throw InternalException("Copy-on-write DELETE: matched rows without an Iceberg scan of the matching rows");
		}
		for (auto &file : files) {
			AddReplacedFile(result, file, *collector.match_file_list);
		}
		return result;
	}

private:
	//! Every live row of a touched file is either deleted or kept. `deletee` may list a row twice, the probe side
	//! cannot, so this is the number of distinct deleted rows.
	idx_t GetDeletedCount() const {
		if (collector.GetDeletes().files.empty()) {
			return 0;
		}
		auto touched = touched_rows.GetCount();
		auto kept = kept_rows.GetCount();
		if (kept > touched) {
			throw InternalException("Copy-on-write DELETE: kept %llu rows of the %llu rows of the touched files", kept,
			                        touched);
		}
		return touched - kept;
	}

	IcebergTable &table;
	IcebergCopyOnWriteDeleteCollector &collector;
	//! The rows of the touched files (the semi join's output), and the rows written back (the anti join's output)
	IcebergCopyOnWriteRowCounter &touched_rows;
	IcebergCopyOnWriteRowCounter &kept_rows;
	//! Owns the logical plan the rewritten rows were planned from
	unique_ptr<LogicalOperator> logical_plan;
};

class CopyOnWriteDeleteGlobalState : public GlobalSinkState {
public:
	mutex lock;
	IcebergCopyOnWriteDeletes deletes;
	//! For an UPDATE: the new version of every matched row
	unique_ptr<ColumnDataCollection> updated_rows;
};

class CopyOnWriteDeleteLocalState : public LocalSinkState {
public:
	unordered_map<string, vector<int64_t>> rows_by_file;
	//! For an UPDATE: evaluates the new values of the matched rows
	unique_ptr<ExpressionExecutor> update_executor;
	DataChunk update_chunk;
};

//! Shares the scan of the updated rows between the threads that finish the probe side
class CopyOnWriteUpdateEmitState : public GlobalOperatorState {
public:
	mutex lock;
	bool scan_initialized = false;
	ColumnDataParallelScanState scan;
};

class CopyOnWriteDeleteOperatorState : public OperatorState {
public:
	explicit CopyOnWriteDeleteOperatorState() : keep(STANDARD_VECTOR_SIZE) {
	}

	//! The positions removed from file_name, or nullptr when the file holds no matching row
	optional_ptr<const unordered_set<int64_t>>
	FindDeleted(const unordered_map<string, unordered_set<int64_t>> &rows_by_file, const string_t &file_name) {
		if (!has_last_file || !(file_name == string_t(last_file))) {
			has_last_file = true;
			last_file = file_name.GetString();
			auto entry = rows_by_file.find(last_file);
			last_deleted = entry == rows_by_file.end() ? nullptr : &entry->second;
		}
		return last_deleted;
	}

	SelectionVector keep;
	ColumnDataLocalScanState updated_rows_scan;
	//! The probe side reads one file at a time, so the last lookup is usually the next one
	bool has_last_file = false;
	string last_file;
	//! The positions removed from last_file, or nullptr when last_file holds no matching row
	optional_ptr<const unordered_set<int64_t>> last_deleted;
};

optional_ptr<PhysicalTableScan> FindTableScan(PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::TABLE_SCAN) {
		return plan.Cast<PhysicalTableScan>();
	}
	if (plan.children.size() != 1) {
		return nullptr;
	}
	return FindTableScan(plan.children[0].get());
}

//! The body of the `deletee` CTE: the DELETE's own scan, reduced to the (filename, file_row_number) of each row it
//! matches. DELETE ... USING can match a row more than once; the anti join does not need the pairs to be distinct.
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

		auto &result = planner.Make<IcebergCopyOnWriteDeleteCollector>(types, projection.estimated_cardinality);
		result.children.push_back(projection);
		collector = result.Cast<IcebergCopyOnWriteDeleteCollector>();
		//! The optimizer can remove the scan when no rows match.
		auto match_scan = IcebergDelete::FindIcebergScan(match);
		if (match_scan) {
			auto &bind_data = match_scan->bind_data->Cast<MultiFileBindData>();
			collector->match_file_list = bind_data.file_list->Cast<IcebergMultiFileList>();
		}
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

//! Counts the rows of its child (see IcebergCopyOnWriteRowCounter); its output is its child's
class LogicalCopyOnWriteRowCount : public LogicalExtensionOperator {
public:
	explicit LogicalCopyOnWriteRowCount(unique_ptr<LogicalOperator> child) {
		children.push_back(std::move(child));
	}

	optional_ptr<IcebergCopyOnWriteRowCounter> counter;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);
		auto &result = planner.Make<IcebergCopyOnWriteRowCounter>(child);
		counter = result.Cast<IcebergCopyOnWriteRowCounter>();
		return result;
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

	string GetExtensionName() const override {
		return "iceberg_copy_on_write_row_count";
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};

class RowCounterGlobalState : public GlobalOperatorState {
public:
	atomic<idx_t> count {0};
};

class CollectorGlobalState : public GlobalOperatorState {
public:
	mutex lock;
	IcebergCopyOnWriteDeletedFiles deletes;
};

class CollectorLocalState : public OperatorState {
public:
	//! The files this thread has already recorded, so the global lock is only taken for new ones
	unordered_set<string> seen_files;
	vector<string> new_files;
};

//! Bind `SELECT * FROM <table>`, then add virtual columns to its scan: on V3, the row lineage columns
//! `_row_id` and `_last_updated_sequence_number`, then always `filename` and `file_row_number`. They are added by
//! column id, not by name, so a table column that happens to be called `filename` cannot shadow them.
unique_ptr<LogicalOperator> BindKeepScan(Binder &binder, IcebergTableSchemaVersion &table, bool with_row_lineage) {
	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(make_uniq<StarExpression>());
	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->SetQualifiedName(table.ParentCatalog().GetName(), table.ParentSchema().name, table.name);
	select->from_table = std::move(table_ref);

	auto bound = binder.Bind(*select);
	auto plan = std::move(bound.plan);
	if (plan->type != LogicalOperatorType::LOGICAL_PROJECTION || plan->children.size() != 1 ||
	    plan->children[0]->type != LogicalOperatorType::LOGICAL_GET) {
		throw InternalException("Copy-on-write: expected the table scan to bind as a projection over a scan");
	}
	auto &projection = plan->Cast<LogicalProjection>();
	auto &get = plan->children[0]->Cast<LogicalGet>();
	vector<column_t> virtual_columns;
	if (with_row_lineage) {
		virtual_columns.push_back(COLUMN_IDENTIFIER_ROW_ID);
		virtual_columns.push_back(IcebergMultiFileReader::COLUMN_IDENTIFIER_LAST_SEQUENCE_NUMBER);
	}
	virtual_columns.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILENAME);
	virtual_columns.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
	for (auto virtual_column : virtual_columns) {
		auto entry = get.virtual_columns.find(virtual_column);
		if (entry == get.virtual_columns.end()) {
			throw InternalException("Copy-on-write: the Iceberg scan does not expose virtual column %d",
			                        static_cast<int64_t>(virtual_column));
		}
		auto &column_ids = get.GetMutableColumnIds();
		column_ids.emplace_back(virtual_column);
		ColumnBinding binding(get.table_index, ProjectionIndex(column_ids.size() - 1));
		projection.expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(entry->second.name, entry->second.type, binding));
	}
	if (with_row_lineage) {
		//! This scan is bound after IcebergPlannerRoutine runs, so do what it does for a scan of the lineage columns:
		//! server scan plans carry no data sequence numbers, so inherited ones need manifest-based planning.
		auto &scan_planner =
		    get.bind_data->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>().GetScanPlanner();
		scan_planner.RequireRowIds();
		scan_planner.DisableServerSidePlanning();
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

//! Passes the kept rows of a V3 copy-on-write DELETE through, refusing any whose lineage is still NULL. Such a row
//! was inserted earlier in this transaction, and a NULL written here would inherit from the replacement file,
//! changing the identity or last update of an unchanged row between the two snapshots published at commit.
class CopyOnWriteLineageCheck : public PhysicalOperator {
public:
	CopyOnWriteLineageCheck(PhysicalPlan &physical_plan, PhysicalOperator &child)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, child.GetTypes(),
	                       child.estimated_cardinality) {
		children.push_back(child);
	}

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override {
		//! The kept rows end with _row_id and _last_updated_sequence_number
		auto column_count = input.ColumnCount();
		D_ASSERT(column_count >= 2);
		if (input.size() && (VectorOperations::HasNull(input.data[column_count - 2]) ||
		                     VectorOperations::HasNull(input.data[column_count - 1]))) {
			throw InvalidInputException("Copy-on-write DELETE cannot preserve uncommitted row lineage. "
			                            "Commit the earlier changes to this table before running the DELETE.");
		}
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	bool ParallelOperator() const override {
		return true;
	}

	string GetName() const override {
		return "ICEBERG_COPY_ON_WRITE_LINEAGE_CHECK";
	}
};

} // namespace

PhysicalOperator &IcebergCopyOnWriteDelete::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                       LogicalDelete &op, IcebergTableSchemaVersion &table) {
	auto &metadata = table.table_info.GetMetadata();
	//! A V3 DELETE carries each kept row's _row_id and _last_updated_sequence_number into the new files.
	//! Rows inserted in this transaction carry NULL until the commit assigns their lineage.
	const bool with_row_lineage = metadata.iceberg_version >= 3;

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

	//! keep: the whole table again (on V3 followed by its row lineage columns), with its filename and file_row_number
	//! as the last two columns
	auto binder = Binder::CreateBinder(context);
	auto keep = BindKeepScan(*binder, table, with_row_lineage);
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
	//! count the rows of the touched files, before the anti join removes the deleted ones
	auto touched_rows = make_uniq<LogicalCopyOnWriteRowCount>(std::move(semi_join));
	auto &touched_rows_ref = *touched_rows;
	anti_join->AddChild(std::move(touched_rows));
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
	if (!matches_ref.collector || !touched_rows_ref.counter) {
		throw InternalException("Copy-on-write DELETE: the deleted rows are not collected");
	}
	auto &kept_rows_counter =
	    planner.Make<IcebergCopyOnWriteRowCounter>(rows_plan).Cast<IcebergCopyOnWriteRowCounter>();

	//! An earlier V3 snapshot in this transaction will assign any lineage that still reads NULL.
	reference<PhysicalOperator> kept_rows = kept_rows_counter;
	auto &transaction_data = table.table_info.transaction_data;
	if (with_row_lineage && transaction_data && transaction_data->has_pending_v3_snapshot) {
		kept_rows = planner.Make<CopyOnWriteLineageCheck>(kept_rows_counter);
	}

	//! Use the same replacement-file writer and snapshot staging as maintenance rewrites.
	auto &rewrite = planner
	                    .Make<CopyOnWriteDeleteRewrite>(table.table_info, *matches_ref.collector,
	                                                    *touched_rows_ref.counter, kept_rows_counter, std::move(plan))
	                    .Cast<CopyOnWriteDeleteRewrite>();
	rewrite.PlanCopy(context, planner, metadata, kept_rows.get(), optional_idx(),
	                 with_row_lineage ? IcebergInsertVirtualColumns::WRITE_ROW_ID_AND_SEQUENCE_NUMBER
	                                  : IcebergInsertVirtualColumns::NONE);
	return rewrite;
}

//===--------------------------------------------------------------------===//
// Copy-on-write UPDATE
//===--------------------------------------------------------------------===//
IcebergCopyOnWriteDeleteFilter::IcebergCopyOnWriteDeleteFilter(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                               PhysicalOperator &keep_plan,
                                                               PhysicalOperator &match_plan,
                                                               vector<idx_t> match_row_id_indexes)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types),
                       keep_plan.estimated_cardinality),
      match_row_id_indexes(std::move(match_row_id_indexes)) {
	children.push_back(keep_plan);
	children.push_back(match_plan);
}

PhysicalOperator &IcebergCopyOnWriteDeleteFilter::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                             LogicalUpdate &op, IcebergTableSchemaVersion &table) {
	//! Build side: the UPDATE's own scan. An Iceberg UPDATE is bound as a delete and an insert, so its expressions
	//! give the new value of every table column, followed in the scan's output by filename and file_row_number.
	auto &match_plan = planner.CreatePlan(*op.children[0]);
	auto match_column_count = match_plan.GetTypes().size();
	vector<idx_t> match_row_id_indexes {match_column_count - 2, match_column_count - 1};

	auto &columns = table.table_info.GetMetadata().GetLatestSchema().columns;
	if (op.columns.size() != columns.size()) {
		throw InternalException(
		    "Copy-on-write UPDATE: expected a new value for each of the %llu table columns, got %llu", columns.size(),
		    op.columns.size());
	}
	unordered_map<idx_t, idx_t> expression_index_by_column;
	for (idx_t i = 0; i < op.columns.size(); i++) {
		expression_index_by_column[op.columns[i].index] = i;
	}
	vector<unique_ptr<Expression>> update_expressions;
	for (idx_t column = 0; column < op.columns.size(); column++) {
		auto expression_index = expression_index_by_column.at(column);
		auto expression = op.expressions[expression_index]->Copy();
		if (expression->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			expression = op.bound_defaults[expression_index]->Copy();
		}
		update_expressions.push_back(std::move(expression));
	}
	return PlanRewrite(context, planner, table, match_plan, std::move(match_row_id_indexes),
	                   std::move(update_expressions));
}

PhysicalOperator &IcebergCopyOnWriteDeleteFilter::PlanRewrite(ClientContext &context, PhysicalPlanGenerator &planner,
                                                              IcebergTableSchemaVersion &table,
                                                              PhysicalOperator &match_plan,
                                                              vector<idx_t> match_row_id_indexes,
                                                              vector<unique_ptr<Expression>> update_expressions) {
	auto &metadata = table.table_info.GetMetadata();
	//! A V3 DELETE carries each kept row's _row_id and _last_updated_sequence_number into the new files.
	//! Rows inserted in this transaction carry NULL until the commit assigns their lineage.
	const bool with_row_lineage = metadata.iceberg_version >= 3;
	D_ASSERT(!with_row_lineage || update_expressions.empty());

	//! Probe side: the whole table again, so the rows of each matched file that survive can be rewritten. It is
	//! planned here, after optimization, like PhysicalPlanGenerator::Plan does for a full statement.
	auto binder = Binder::CreateBinder(context);
	auto keep_logical_plan = BindKeepScan(*binder, table, with_row_lineage);
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*keep_logical_plan);
	keep_logical_plan->ResolveOperatorTypes();
	auto &keep_plan = planner.CreatePlan(*keep_logical_plan);

	auto keep_types = keep_plan.GetTypes();
	D_ASSERT(keep_types.size() > 2);
	vector<LogicalType> output_types(keep_types.begin(), keep_types.end() - 2);
	//! The updated rows are emitted in the same chunks as the kept ones
	for (idx_t i = 0; i < update_expressions.size(); i++) {
		if (update_expressions[i]->GetReturnType() != output_types[i]) {
			update_expressions[i] =
			    BoundCastExpression::AddCastToType(context, std::move(update_expressions[i]), output_types[i]);
		}
	}

	auto &filter = planner
	                   .Make<IcebergCopyOnWriteDeleteFilter>(std::move(output_types), keep_plan, match_plan,
	                                                         std::move(match_row_id_indexes))
	                   .Cast<IcebergCopyOnWriteDeleteFilter>();
	filter.keep_logical_plan = std::move(keep_logical_plan);
	filter.update_expressions = std::move(update_expressions);
	auto &transaction_data = table.table_info.transaction_data;
	filter.require_assigned_lineage = transaction_data && transaction_data->has_pending_v3_snapshot;
	//! The optimizer can remove the scan when no rows match.
	auto match_scan = IcebergDelete::FindIcebergScan(match_plan);
	if (match_scan) {
		auto &bind_data = match_scan->bind_data->Cast<MultiFileBindData>();
		filter.match_file_list = bind_data.file_list->Cast<IcebergMultiFileList>();
	}

	auto keep_scan = FindTableScan(keep_plan);
	if (keep_scan) {
		auto &column_ids = keep_scan->column_ids;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (column_ids[i].IsVirtualColumn() &&
			    column_ids[i].GetPrimaryIndex() == MultiFileReader::COLUMN_IDENTIFIER_FILENAME) {
				filter.keep_scan = keep_scan;
				filter.keep_scan_filename_index = ProjectionIndex(i);
				if (!keep_scan->dynamic_filters) {
					keep_scan->dynamic_filters = make_shared_ptr<DynamicTableFilterSet>();
				}
				break;
			}
		}
	}

	//! Use the same replacement-file writer and snapshot staging as maintenance rewrites.
	auto &rewrite = planner.Make<CopyOnWriteUpdateRewrite>(table.table_info, filter).Cast<CopyOnWriteUpdateRewrite>();
	rewrite.PlanCopy(context, planner, metadata, filter, optional_idx(),
	                 with_row_lineage ? IcebergInsertVirtualColumns::WRITE_ROW_ID_AND_SEQUENCE_NUMBER
	                                  : IcebergInsertVirtualColumns::NONE);
	return rewrite;
}

//===--------------------------------------------------------------------===//
// Sink (build side)
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> IcebergCopyOnWriteDeleteFilter::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_uniq<CopyOnWriteDeleteGlobalState>();
	if (!update_expressions.empty()) {
		result->updated_rows = make_uniq<ColumnDataCollection>(context, types);
	}
	return std::move(result);
}

unique_ptr<LocalSinkState> IcebergCopyOnWriteDeleteFilter::GetLocalSinkState(ExecutionContext &context) const {
	auto result = make_uniq<CopyOnWriteDeleteLocalState>();
	if (!update_expressions.empty()) {
		result->update_executor = make_uniq<ExpressionExecutor>(context.client, update_expressions);
		result->update_chunk.Initialize(context.client, types);
	}
	return std::move(result);
}

SinkResultType IcebergCopyOnWriteDeleteFilter::Sink(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSinkInput &input) const {
	auto &local_state = input.local_state.Cast<CopyOnWriteDeleteLocalState>();

	UnifiedVectorFormat file_name_data;
	chunk.data[match_row_id_indexes[0]].ToUnifiedFormat(file_name_data);
	auto file_names = UnifiedVectorFormat::GetData<string_t>(file_name_data);
	UnifiedVectorFormat row_number_data;
	chunk.data[match_row_id_indexes[1]].ToUnifiedFormat(row_number_data);
	auto row_numbers = UnifiedVectorFormat::GetData<int64_t>(row_number_data);

	optional_ptr<vector<int64_t>> current_rows;
	string current_file;
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto file_idx = file_name_data.sel->get_index(i);
		if (!file_name_data.validity.RowIsValid(file_idx)) {
			throw InternalException("Filename cannot be NULL!");
		}
		auto file_name = file_names[file_idx].GetString();
		if (!current_rows || file_name != current_file) {
			current_file = file_name;
			current_rows = local_state.rows_by_file[current_file];
		}
		current_rows->push_back(row_numbers[row_number_data.sel->get_index(i)]);
	}

	if (local_state.update_executor) {
		auto &global_state = input.global_state.Cast<CopyOnWriteDeleteGlobalState>();
		local_state.update_chunk.Reset();
		local_state.update_executor->Execute(chunk, local_state.update_chunk);
		lock_guard<mutex> guard(global_state.lock);
		global_state.updated_rows->Append(local_state.update_chunk);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType IcebergCopyOnWriteDeleteFilter::Combine(ExecutionContext &context,
                                                              OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<CopyOnWriteDeleteGlobalState>();
	auto &local_state = input.local_state.Cast<CopyOnWriteDeleteLocalState>();
	lock_guard<mutex> guard(global_state.lock);
	for (auto &entry : local_state.rows_by_file) {
		auto &deleted = global_state.deletes.rows_by_file[entry.first];
		for (auto row_number : entry.second) {
			if (deleted.insert(row_number).second) {
				global_state.deletes.deleted_count++;
			} else if (!update_expressions.empty()) {
				//! DELETE ... USING can match the same target row more than once; an UPDATE would give it two values
				throw NotImplementedException(
				    "The same row was updated multiple times - this is not (yet) supported in Iceberg. Eliminate "
				    "duplicate matches prior to running the UPDATE");
			}
		}
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType IcebergCopyOnWriteDeleteFilter::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                          OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<CopyOnWriteDeleteGlobalState>();
	PushKeepScanFilter(global_state.deletes);
	return SinkFinalizeType::READY;
}

//! Restrict the probe-side scan to the matched files. The filter is on the constant `filename` column, so the
//! scan evaluates it once per file and skips every other file without reading it. Execute filters by file too,
//! so this is only an optimization.
void IcebergCopyOnWriteDeleteFilter::PushKeepScanFilter(const IcebergCopyOnWriteDeletes &deletes) const {
	if (!keep_scan || !keep_scan->dynamic_filters) {
		return;
	}
	auto column = make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, idx_t(0));
	unique_ptr<Expression> filter_expression;
	if (deletes.rows_by_file.empty()) {
		//! Nothing matched, so no file is rewritten: '' is never the path of a data file
		filter_expression = ExpressionFilter::CreateInExpression(std::move(column), {Value("")});
	} else {
		vector<Value> files;
		files.reserve(deletes.rows_by_file.size());
		for (auto &entry : deletes.rows_by_file) {
			files.emplace_back(entry.first);
		}
		filter_expression = ExpressionFilter::CreateInExpression(std::move(column), std::move(files));
	}
	keep_scan->dynamic_filters->PushFilter(*keep_scan, keep_scan_filename_index,
	                                       make_uniq<ExpressionFilter>(std::move(filter_expression)));
}

const IcebergCopyOnWriteDeletes &IcebergCopyOnWriteDeleteFilter::GetDeletes() const {
	if (!sink_state) {
		throw InternalException("Copy-on-write DELETE: the matched rows were read before they were collected");
	}
	return sink_state->Cast<CopyOnWriteDeleteGlobalState>().deletes;
}

//===--------------------------------------------------------------------===//
// Execute (probe side)
//===--------------------------------------------------------------------===//
unique_ptr<OperatorState> IcebergCopyOnWriteDeleteFilter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<CopyOnWriteDeleteOperatorState>();
}

OperatorResultType IcebergCopyOnWriteDeleteFilter::Execute(ExecutionContext &context, DataChunk &input,
                                                           DataChunk &chunk, GlobalOperatorState &gstate,
                                                           OperatorState &state_p) const {
	auto &state = state_p.Cast<CopyOnWriteDeleteOperatorState>();
	auto &rows_by_file = GetDeletes().rows_by_file;

	//! The probe side emits the columns this operator outputs (the table's columns, then on V3 _row_id and
	//! _last_updated_sequence_number), followed by filename and file_row_number
	auto column_count = types.size();
	D_ASSERT(input.ColumnCount() == column_count + 2);

	auto &file_name_vector = input.data[column_count];
	UnifiedVectorFormat file_name_data;
	file_name_vector.ToUnifiedFormat(file_name_data);
	auto file_names = UnifiedVectorFormat::GetData<string_t>(file_name_data);
	UnifiedVectorFormat row_number_data;
	input.data[column_count + 1].ToUnifiedFormat(row_number_data);
	auto row_numbers = UnifiedVectorFormat::GetData<int64_t>(row_number_data);

	//! A file with no matching row stays as it is, so none of its rows are kept for the rewrite
	idx_t kept = 0;
	if (file_name_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		//! The scan emits one file per chunk as a constant: look it up once, not for every row
		if (ConstantVector::IsNull(file_name_vector)) {
			throw InternalException("Filename cannot be NULL!");
		}
		auto deleted = state.FindDeleted(rows_by_file, file_names[0]);
		if (deleted) {
			for (idx_t i = 0; i < input.size(); i++) {
				if (!deleted->count(row_numbers[row_number_data.sel->get_index(i)])) {
					state.keep.set_index(kept++, i);
				}
			}
		}
	} else {
		for (idx_t i = 0; i < input.size(); i++) {
			auto file_idx = file_name_data.sel->get_index(i);
			if (!file_name_data.validity.RowIsValid(file_idx)) {
				throw InternalException("Filename cannot be NULL!");
			}
			auto deleted = state.FindDeleted(rows_by_file, file_names[file_idx]);
			if (!deleted || deleted->count(row_numbers[row_number_data.sel->get_index(i)])) {
				continue;
			}
			state.keep.set_index(kept++, i);
		}
	}

	for (idx_t col = 0; col < column_count; col++) {
		chunk.data[col].Slice(input.data[col], state.keep, kept);
	}
	chunk.SetChildCardinality(kept);
	//! A NULL written here would inherit from the replacement file, changing the identity or last update
	//! of an unchanged row between the two snapshots published at commit.
	if (require_assigned_lineage && kept &&
	    (VectorOperations::HasNull(chunk.data[column_count - 2]) ||
	     VectorOperations::HasNull(chunk.data[column_count - 1]))) {
		throw InvalidInputException("Copy-on-write DELETE cannot preserve uncommitted row lineage. "
		                            "Commit the earlier changes to this table before running the DELETE.");
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalOperatorState> IcebergCopyOnWriteDeleteFilter::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<CopyOnWriteUpdateEmitState>();
}

OperatorFinalizeResultType IcebergCopyOnWriteDeleteFilter::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                                        GlobalOperatorState &gstate_p,
                                                                        OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<CopyOnWriteUpdateEmitState>();
	auto &state = state_p.Cast<CopyOnWriteDeleteOperatorState>();
	auto &updated_rows = *sink_state->Cast<CopyOnWriteDeleteGlobalState>().updated_rows;
	{
		lock_guard<mutex> guard(gstate.lock);
		if (!gstate.scan_initialized) {
			updated_rows.InitializeScan(gstate.scan);
			gstate.scan_initialized = true;
		}
	}
	//! Every thread that finishes the probe side helps emit the updated rows, each of them exactly once
	if (updated_rows.Scan(gstate.scan, state.updated_rows_scan, chunk)) {
		return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
	}
	return OperatorFinalizeResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Pipelines
//===--------------------------------------------------------------------===//
void IcebergCopyOnWriteDeleteFilter::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	//! Like a join: the build side (the matching rows) completes before the probe side (the rewrite) starts
	PhysicalJoin::BuildJoinPipelines(current, meta_pipeline, *this);
}

vector<const_reference<PhysicalOperator>> IcebergCopyOnWriteDeleteFilter::GetSources() const {
	return children[0].get().GetSources();
}

string IcebergCopyOnWriteDeleteFilter::GetName() const {
	return update_expressions.empty() ? "ICEBERG_COPY_ON_WRITE_DELETE" : "ICEBERG_COPY_ON_WRITE_UPDATE";
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
		for (auto &file : state.new_files) {
			gstate.deletes.files.insert(std::move(file));
		}
	}
	state.new_files.clear();

	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

const IcebergCopyOnWriteDeletedFiles &IcebergCopyOnWriteDeleteCollector::GetDeletes() const {
	if (!op_state) {
		throw InternalException("Copy-on-write DELETE: the deleted rows were read before they were collected");
	}
	return op_state->Cast<CollectorGlobalState>().deletes;
}

string IcebergCopyOnWriteDeleteCollector::GetName() const {
	return "ICEBERG_COPY_ON_WRITE_DELETED_ROWS";
}

//===--------------------------------------------------------------------===//
// Row counter
//===--------------------------------------------------------------------===//
IcebergCopyOnWriteRowCounter::IcebergCopyOnWriteRowCounter(PhysicalPlan &physical_plan, PhysicalOperator &child)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, child.GetTypes(), child.estimated_cardinality) {
	children.push_back(child);
}

unique_ptr<GlobalOperatorState> IcebergCopyOnWriteRowCounter::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<RowCounterGlobalState>();
}

OperatorResultType IcebergCopyOnWriteRowCounter::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                         GlobalOperatorState &gstate, OperatorState &state) const {
	gstate.Cast<RowCounterGlobalState>().count += input.size();
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

idx_t IcebergCopyOnWriteRowCounter::GetCount() const {
	if (!op_state) {
		throw InternalException("Copy-on-write DELETE: the rows were read before they were counted");
	}
	return op_state->Cast<RowCounterGlobalState>().count.load();
}

string IcebergCopyOnWriteRowCounter::GetName() const {
	return "ICEBERG_COPY_ON_WRITE_ROW_COUNT";
}

} // namespace duckdb
