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
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
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
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
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
void AddReplacedFile(IcebergFileRewriteInput &result, const string &file, const IcebergMultiFileList &target_files) {
	result.replaced_files.emplace_back(file);
	auto existing_deletes = target_files.GetExistingPositionalDeleteData(file);
	if (existing_deletes) {
		for (auto &delete_file : existing_deletes->source_files) {
			result.replaced_files.push_back(delete_file);
		}
	}
}

class CopyOnWriteUpdateRewrite : public IcebergFileRewrite {
public:
	CopyOnWriteUpdateRewrite(PhysicalPlan &physical_plan, IcebergTable &table,
	                         IcebergCopyOnWriteDeleteFilter &delete_filter, const IcebergMultiFileList &target_files)
	    : IcebergFileRewrite(physical_plan, {LogicalType::BIGINT}, 1, IcebergWriteOperation::COPY_ON_WRITE_DELETE),
	      table(table), delete_filter(delete_filter), target_files(target_files) {
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		return make_uniq<IcebergFileRewriteGlobalState>(context, table);
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(delete_filter.GetResultCount())));
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
		for (auto &entry : rows_by_file) {
			AddReplacedFile(result, entry.first, target_files);
		}
		return result;
	}

	void StageRewrite(IcebergTransactionData &transaction_data, const IcebergFileRewriteGlobalState &state,
	                  IcebergSnapshotOperationType operation, vector<IcebergManifestEntry> &&new_entries,
	                  IcebergManifestDeletes &&replaced_files) const override {
		//! A MERGE's rewrite joins the snapshot its actions stage
		bool merge_action = delete_filter.merge_rows != nullptr;
		if (operation == IcebergSnapshotOperationType::DELETE) {
			transaction_data.AddDeleteSnapshot(partitioned_manifest_entry_map_t(), std::move(replaced_files),
			                                   merge_action);
		} else {
			transaction_data.AddSnapshot(operation, std::move(new_entries), std::move(replaced_files), merge_action);
		}
	}

private:
	IcebergTable &table;
	IcebergCopyOnWriteDeleteFilter &delete_filter;
	const IcebergMultiFileList &target_files;
};

class CopyOnWriteDeleteRewrite : public IcebergFileRewrite {
public:
	CopyOnWriteDeleteRewrite(PhysicalPlan &physical_plan, IcebergTable &table,
	                         IcebergCopyOnWriteRowCounter &touched_rows, const IcebergMultiFileList &target_files,
	                         unique_ptr<LogicalOperator> logical_plan)
	    : IcebergFileRewrite(physical_plan, {LogicalType::BIGINT}, 1, IcebergWriteOperation::COPY_ON_WRITE_DELETE),
	      table(table), touched_rows(touched_rows), target_files(target_files), logical_plan(std::move(logical_plan)) {
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		return make_uniq<IcebergFileRewriteGlobalState>(context, table);
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		auto &state = sink_state->Cast<IcebergFileRewriteGlobalState>();
		chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(GetDeletedCount(state.insert_count.load()))));
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
		for (auto &file : touched_rows.GetFiles()) {
			AddReplacedFile(result, file, target_files);
		}
		return result;
	}

private:
	//! Every live row of a touched file is either deleted or kept. `deletee` may list a row twice, the probe side
	//! cannot, so this is the number of distinct deleted rows. The kept rows are the rows the writer wrote.
	idx_t GetDeletedCount(idx_t kept) const {
		if (touched_rows.GetFiles().empty()) {
			return 0;
		}
		auto touched = touched_rows.GetCount();
		if (kept > touched) {
			throw InternalException("Copy-on-write DELETE: kept %llu rows of the %llu rows of the touched files", kept,
			                        touched);
		}
		return touched - kept;
	}

	IcebergTable &table;
	//! The rows of the touched files (the join with dict's output), which also records the files
	IcebergCopyOnWriteRowCounter &touched_rows;
	//! The file list of the keep scan, which reads every touched file. It has loaded their delete files, which are
	//! removed together with them. Not the DELETE's own scan: in a DELETE ... USING, the scan that a matched row's
	//! filename comes from can be the source's.
	const IcebergMultiFileList &target_files;
	//! Owns the logical plan the rewritten rows were planned from
	unique_ptr<LogicalOperator> logical_plan;
};

class CopyOnWriteDeleteGlobalState : public GlobalSinkState {
public:
	mutex lock;
	IcebergCopyOnWriteDeletes deletes;
	//! For an UPDATE: the new version of every matched row
	unique_ptr<ColumnDataCollection> updated_rows;
	//! For a MERGE: the row count it reported
	idx_t merged_count = 0;
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

//! The body of the `raw` CTE: the DELETE's own scan, reduced to the (filename, file_row_number) of each row it matches.
//! DELETE ... USING can match a row more than once; the anti join does not need the pairs to be distinct. The DELETE's
//! plan is already optimized and resolved, so it is planned as is rather than as a child of this tree.
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

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &match = planner.CreatePlan(match_plan);
		vector<unique_ptr<Expression>> select_list;
		for (idx_t i = 0; i < row_id_indexes.size(); i++) {
			select_list.push_back(make_uniq<BoundReferenceExpression>(row_id_types[i], row_id_indexes[i]));
		}
		auto &projection =
		    planner.Make<PhysicalProjection>(row_id_types, std::move(select_list), match.estimated_cardinality);
		projection.children.push_back(match);
		return projection;
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
	explicit LogicalCopyOnWriteRowCount(unique_ptr<LogicalOperator> child, optional_idx file_column = optional_idx())
	    : file_column(file_column) {
		children.push_back(std::move(child));
	}

	//! The column of the child with the filename, if the files are to be recorded too
	optional_idx file_column;
	optional_ptr<IcebergCopyOnWriteRowCounter> counter;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);
		auto &result = planner.Make<IcebergCopyOnWriteRowCounter>(child, file_column);
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
	mutex lock;
	unordered_set<string> files;
};

class RowCounterLocalState : public OperatorState {
public:
	//! The files this thread has recorded, so the global lock is only taken for a new one
	unordered_set<string> seen_files;
	//! The scan reads one file at a time, so the last file is usually the next one
	bool has_last_file = false;
	string last_file;
};

//! row_number() OVER (), bound by DuckDB's binder as in `SELECT row_number() OVER ()`. It refers to no column, so it
//! can be used in any window operator.
unique_ptr<Expression> BindRowNumber(ClientContext &context) {
	auto select = make_uniq<SelectNode>();
	Parser parser(context);
	auto expressions = parser.ParseExpressionList("row_number() OVER ()");
	D_ASSERT(expressions.size() == 1);
	select->select_list.push_back(std::move(expressions[0]));
	select->from_table = make_uniq<EmptyTableRef>();
	auto binder = Binder::CreateBinder(context);
	auto bound = binder->Bind(*select);
	reference<LogicalOperator> op = *bound.plan;
	while (op.get().type != LogicalOperatorType::LOGICAL_WINDOW) {
		if (op.get().children.size() != 1) {
			throw InternalException("Copy-on-write DELETE: row_number() did not bind as a window");
		}
		op = *op.get().children[0];
	}
	auto &window = op.get().Cast<LogicalWindow>();
	D_ASSERT(window.expressions.size() == 1);
	return window.expressions[0]->Copy();
}

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
	auto &keep_get = keep->children[0]->Cast<LogicalGet>();
	auto &target_files = keep_get.bind_data->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>();
	auto keep_index = keep->Cast<LogicalProjection>().table_index;
	auto column_count = keep->Cast<LogicalProjection>().expressions.size() - 2;
	vector<LogicalType> column_types;
	for (auto &expr : keep->Cast<LogicalProjection>().expressions) {
		column_types.push_back(expr->GetReturnType());
	}

	//! The CTE indexes have to be distinct from any CTE in the DELETE's plan: they share the physical planner
	idx_t max_index = 0;
	MaxTableIndex(match_plan, max_index);
	MaxTableIndex(*keep, max_index);
	TableIndex raw_index(MaxValue(max_index + 1, binder->GenerateTableIndex().index));
	TableIndex dict_index(raw_index.index + 1);
	auto raw_ref = [&]() {
		return make_uniq<LogicalCTERef>(binder->GenerateTableIndex(), raw_index, row_id_types,
		                                vector<Identifier> {"filename", "file_row_number"});
	};
	vector<LogicalType> dict_types {LogicalType::VARCHAR, LogicalType::BIGINT};
	auto dict_ref = [&]() {
		return make_uniq<LogicalCTERef>(binder->GenerateTableIndex(), dict_index, dict_types,
		                                vector<Identifier> {"filename", "file_no"});
	};

	//! dict: SELECT filename, row_number() OVER () AS file_no FROM (SELECT DISTINCT filename FROM raw), one row per
	//! touched file
	auto paths_ref = raw_ref();
	auto paths_ref_index = paths_ref->table_index;
	auto paths_index = binder->GenerateTableIndex();
	vector<unique_ptr<Expression>> paths_list;
	paths_list.push_back(ColumnRef(row_id_types[0], paths_ref_index, 0));
	auto paths = make_uniq<LogicalProjection>(paths_index, std::move(paths_list));
	paths->AddChild(std::move(paths_ref));
	vector<unique_ptr<Expression>> distinct_targets;
	distinct_targets.push_back(ColumnRef(row_id_types[0], paths_index, 0));
	auto distinct_paths = make_uniq<LogicalDistinct>(std::move(distinct_targets), DistinctType::DISTINCT);
	distinct_paths->AddChild(std::move(paths));
	auto window_index = binder->GenerateTableIndex();
	auto numbering = make_uniq<LogicalWindow>(window_index);
	numbering->expressions.push_back(BindRowNumber(context));
	numbering->AddChild(std::move(distinct_paths));
	vector<unique_ptr<Expression>> dict_list;
	dict_list.push_back(ColumnRef(row_id_types[0], paths_index, 0));
	dict_list.push_back(ColumnRef(dict_types[1], window_index, 0));
	unique_ptr<LogicalOperator> dict = make_uniq<LogicalProjection>(binder->GenerateTableIndex(), std::move(dict_list));
	dict->AddChild(std::move(numbering));

	//! deletee: SELECT dict.file_no, raw.file_row_number FROM raw JOIN dict USING (filename). dict, one row per
	//! file, is the build side.
	auto deletee_raw = raw_ref();
	auto deletee_raw_index = deletee_raw->table_index;
	auto deletee_dict = dict_ref();
	auto deletee_dict_index = deletee_dict->table_index;
	auto numbering_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	numbering_join->conditions.emplace_back(ColumnRef(row_id_types[0], deletee_raw_index, 0),
	                                        ColumnRef(dict_types[0], deletee_dict_index, 0),
	                                        ExpressionType::COMPARE_EQUAL);
	numbering_join->AddChild(std::move(deletee_raw));
	numbering_join->AddChild(std::move(deletee_dict));
	auto deletee_index = binder->GenerateTableIndex();
	vector<unique_ptr<Expression>> deletee_list;
	deletee_list.push_back(ColumnRef(dict_types[1], deletee_dict_index, 1));
	deletee_list.push_back(ColumnRef(row_id_types[1], deletee_raw_index, 1));
	auto deletee = make_uniq<LogicalProjection>(deletee_index, std::move(deletee_list));
	deletee->AddChild(std::move(numbering_join));

	//! keep JOIN dict ON keep.filename = dict.filename: dict has each file once, so this only adds the file number to
	//! the rows of the touched files
	auto touched = dict_ref();
	auto touched_index = touched->table_index;
	auto touched_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	auto &touched_join_ref = *touched_join;
	touched_join->conditions.emplace_back(ColumnRef(row_id_types[0], keep_index, column_count),
	                                      ColumnRef(dict_types[0], touched_index, 0), ExpressionType::COMPARE_EQUAL);
	touched_join->AddChild(std::move(keep));
	touched_join->AddChild(std::move(touched));

	//! ANTI JOIN deletee ON dict.file_no = deletee.file_no AND keep.file_row_number = deletee.file_row_number
	auto anti_join = make_uniq<LogicalComparisonJoin>(JoinType::ANTI);
	anti_join->conditions.emplace_back(ColumnRef(dict_types[1], touched_index, 1),
	                                   ColumnRef(dict_types[1], deletee_index, 0), ExpressionType::COMPARE_EQUAL);
	anti_join->conditions.emplace_back(ColumnRef(row_id_types[1], keep_index, column_count + 1),
	                                   ColumnRef(row_id_types[1], deletee_index, 1), ExpressionType::COMPARE_EQUAL);
	//! count the rows of the touched files, before the anti join removes the deleted ones, and record the files:
	//! every file of dict has a live row here, including a file whose rows are all deleted
	auto touched_rows = make_uniq<LogicalCopyOnWriteRowCount>(std::move(touched_join), column_count);
	auto &touched_rows_ref = *touched_rows;
	anti_join->AddChild(std::move(touched_rows));
	anti_join->AddChild(std::move(deletee));

	//! SELECT keep.*
	vector<unique_ptr<Expression>> keep_list;
	for (idx_t i = 0; i < column_count; i++) {
		keep_list.push_back(ColumnRef(column_types[i], keep_index, i));
	}
	unique_ptr<LogicalOperator> rows = make_uniq<LogicalProjection>(binder->GenerateTableIndex(), std::move(keep_list));
	rows->AddChild(std::move(anti_join));

	//! The join with dict restricts the keep scan to the touched files at runtime: its build side becomes a filter on
	//! the constant `filename` column, which the scan evaluates once per file, skipping every other file unread.
	Optimizer optimizer(*binder, context);
	JoinFilterPushdownOptimizer join_filter_pushdown(optimizer);
	join_filter_pushdown.VisitOperator(*rows);
	if (touched_join_ref.filter_pushdown) {
		//! The DELETE's predicate usually selects few rows, so the touched files are worth filtering on
		touched_join_ref.filter_pushdown->build_side_has_filter = true;
	}

	//! The DELETE's plan is resolved already; only the new operators above the raw CTE body need it.
	ColumnBindingResolver dict_resolver;
	dict_resolver.VisitOperator(*dict);
	ColumnBindingResolver rows_resolver;
	rows_resolver.VisitOperator(*rows);

	auto with_dict = make_uniq<LogicalMaterializedCTE>("dict", dict_index, 2, std::move(dict), std::move(rows),
	                                                   CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	unique_ptr<LogicalOperator> plan = make_uniq<LogicalMaterializedCTE>(
	    "raw", raw_index, 2, std::move(matches), std::move(with_dict), CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	plan->ResolveOperatorTypes();
	auto &rows_plan = planner.CreatePlan(*plan);
	D_ASSERT(rows_plan.GetTypes().size() == column_count);
	if (!touched_rows_ref.counter) {
		throw InternalException("Copy-on-write DELETE: the deleted rows are not collected");
	}
	//! An earlier V3 snapshot in this transaction will assign any lineage that still reads NULL.
	reference<PhysicalOperator> kept_rows = rows_plan;
	auto &transaction_data = table.table_info.transaction_data;
	if (with_row_lineage && transaction_data && transaction_data->has_pending_v3_snapshot) {
		kept_rows = planner.Make<CopyOnWriteLineageCheck>(rows_plan);
	}

	//! Use the same replacement-file writer and snapshot staging as maintenance rewrites.
	auto &rewrite =
	    planner
	        .Make<CopyOnWriteDeleteRewrite>(table.table_info, *touched_rows_ref.counter, target_files, std::move(plan))
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
	//! give the new value of every table column, followed in the scan's output by filename and file_row_number and,
	//! on V3, _row_id (see IcebergTableSchemaVersion::GetRowIdColumns).
	auto &match_plan = planner.CreatePlan(*op.children[0]);
	const bool with_row_lineage = table.table_info.GetMetadata().iceberg_version >= 3;
	auto match_column_count = match_plan.GetTypes().size();
	const idx_t lineage_column_count = with_row_lineage ? 1 : 0;
	D_ASSERT(match_column_count >= 2 + lineage_column_count);
	vector<idx_t> match_row_id_indexes {match_column_count - lineage_column_count - 2,
	                                    match_column_count - lineage_column_count - 1};

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
	if (with_row_lineage) {
		//! SPEC: an updated row keeps its _row_id, and its _last_updated_sequence_number is written as NULL, so it
		//! inherits the sequence number of the snapshot that commits the update. A row inserted earlier in this
		//! transaction has no _row_id yet; it gets one at commit, as with a merge-on-read UPDATE.
		auto row_id_index = match_column_count - 1;
		update_expressions.push_back(
		    make_uniq<BoundReferenceExpression>(match_plan.GetTypes()[row_id_index], row_id_index));
		update_expressions.push_back(make_uniq<BoundConstantExpression>(Value(LogicalType::BIGINT)));
	}
	return PlanRewrite(context, planner, table, match_plan, std::move(match_row_id_indexes),
	                   std::move(update_expressions));
}

PhysicalOperator &IcebergCopyOnWriteDeleteFilter::PlanMerge(ClientContext &context, PhysicalPlanGenerator &planner,
                                                            IcebergTableSchemaVersion &table,
                                                            PhysicalOperator &merge_plan,
                                                            shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows) {
	//! Build side: the MERGE. Its pipelines, including those of its actions, complete before it emits its row count,
	//! so by the time this operator finalizes, every action has handed over the rows it removed.
	return PlanRewrite(context, planner, table, merge_plan, {}, {}, std::move(merge_rows));
}

PhysicalOperator &IcebergCopyOnWriteDeleteFilter::PlanRewrite(ClientContext &context, PhysicalPlanGenerator &planner,
                                                              IcebergTableSchemaVersion &table,
                                                              PhysicalOperator &match_plan,
                                                              vector<idx_t> match_row_id_indexes,
                                                              vector<unique_ptr<Expression>> update_expressions,
                                                              shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows) {
	auto &metadata = table.table_info.GetMetadata();
	//! A V3 rewrite carries each kept row's _row_id and _last_updated_sequence_number into the new files.
	//! Rows inserted in this transaction carry NULL until the commit assigns their lineage.
	const bool with_row_lineage = metadata.iceberg_version >= 3;

	//! Probe side: the whole table again, so the rows of each matched file that survive can be rewritten. It is
	//! planned here, after optimization, like PhysicalPlanGenerator::Plan does for a full statement.
	auto binder = Binder::CreateBinder(context);
	auto keep_logical_plan = BindKeepScan(*binder, table, with_row_lineage);
	//! The keep scan reads the deletes for every matched target file. A joined UPDATE or DELETE can also
	//! project filename and file_row_number from a different table on the match side.
	auto &keep_get = keep_logical_plan->children[0]->Cast<LogicalGet>();
	auto &target_files = keep_get.bind_data->Cast<MultiFileBindData>().file_list->Cast<IcebergMultiFileList>();
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*keep_logical_plan);
	keep_logical_plan->ResolveOperatorTypes();
	auto &keep_plan = planner.CreatePlan(*keep_logical_plan);

	auto keep_types = keep_plan.GetTypes();
	D_ASSERT(keep_types.size() > 2);
	vector<LogicalType> output_types(keep_types.begin(), keep_types.end() - 2);
	//! The updated rows are emitted in the same chunks as the kept ones, so on V3 they end with the lineage columns too
	D_ASSERT(update_expressions.empty() || update_expressions.size() == output_types.size());
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
	filter.merge_rows = merge_rows;
	auto &transaction_data = table.table_info.transaction_data;
	filter.require_assigned_lineage = transaction_data && transaction_data->has_pending_v3_snapshot;

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
	auto &rewrite =
	    planner.Make<CopyOnWriteUpdateRewrite>(table.table_info, filter, target_files).Cast<CopyOnWriteUpdateRewrite>();
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
	if (merge_rows) {
		//! The MERGE emits only its row count; the rows it removed reach merge_rows directly
		auto &global_state = input.global_state.Cast<CopyOnWriteDeleteGlobalState>();
		lock_guard<mutex> guard(global_state.lock);
		for (idx_t i = 0; i < chunk.size(); i++) {
			global_state.merged_count += NumericCast<idx_t>(chunk.GetValue(0, i).GetValue<int64_t>());
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

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
	if (merge_rows) {
		lock_guard<mutex> guard(merge_rows->lock);
		for (auto &entry : merge_rows->deletes.rows_by_file) {
			auto &deleted = global_state.deletes.rows_by_file[entry.first];
			for (auto row_number : entry.second) {
				if (!deleted.insert(NumericCast<int64_t>(row_number)).second) {
					throw NotImplementedException(
					    "The same row was updated multiple times - this is not (yet) supported in Iceberg. Eliminate "
					    "duplicate matches prior to running the MERGE");
				}
			}
			global_state.deletes.deleted_count += entry.second.size();
		}
		merge_rows->deletes.rows_by_file.clear();
	}
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

idx_t IcebergCopyOnWriteDeleteFilter::GetResultCount() const {
	if (merge_rows) {
		return sink_state->Cast<CopyOnWriteDeleteGlobalState>().merged_count;
	}
	return GetDeletes().deleted_count;
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
		auto statement = update_expressions.empty() ? "DELETE" : "UPDATE";
		throw InvalidInputException("Copy-on-write %s cannot preserve uncommitted row lineage. "
		                            "Commit the earlier changes to this table before running the %s.",
		                            statement, statement);
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
	if (merge_rows) {
		return "ICEBERG_COPY_ON_WRITE_MERGE";
	}
	return update_expressions.empty() ? "ICEBERG_COPY_ON_WRITE_DELETE" : "ICEBERG_COPY_ON_WRITE_UPDATE";
}

//===--------------------------------------------------------------------===//
// Row counter
//===--------------------------------------------------------------------===//
IcebergCopyOnWriteRowCounter::IcebergCopyOnWriteRowCounter(PhysicalPlan &physical_plan, PhysicalOperator &child,
                                                           optional_idx file_column)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, child.GetTypes(), child.estimated_cardinality),
      file_column(file_column) {
	children.push_back(child);
}

unique_ptr<GlobalOperatorState> IcebergCopyOnWriteRowCounter::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<RowCounterGlobalState>();
}

unique_ptr<OperatorState> IcebergCopyOnWriteRowCounter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<RowCounterLocalState>();
}

static void RecordFile(RowCounterGlobalState &gstate, RowCounterLocalState &state, const string_t &file_name) {
	if (state.has_last_file && file_name == string_t(state.last_file)) {
		return;
	}
	auto path = file_name.GetString();
	state.has_last_file = true;
	state.last_file = path;
	if (state.seen_files.insert(path).second) {
		lock_guard<mutex> guard(gstate.lock);
		gstate.files.insert(std::move(path));
	}
}

OperatorResultType IcebergCopyOnWriteRowCounter::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                         GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<RowCounterGlobalState>();
	gstate.count += input.size();
	if (file_column.IsValid() && input.size() > 0) {
		auto &state = state_p.Cast<RowCounterLocalState>();
		auto &file_name_vector = input.data[file_column.GetIndex()];
		if (file_name_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			//! The scan emits one file per chunk as a constant: record it once, not for every row
			if (ConstantVector::IsNull(file_name_vector)) {
				throw InternalException("Filename cannot be NULL!");
			}
			RecordFile(gstate, state, ConstantVector::GetData<string_t>(file_name_vector)[0]);
		} else {
			UnifiedVectorFormat file_name_data;
			file_name_vector.ToUnifiedFormat(file_name_data);
			auto file_names = UnifiedVectorFormat::GetData<string_t>(file_name_data);
			for (idx_t i = 0; i < input.size(); i++) {
				auto file_idx = file_name_data.sel->get_index(i);
				if (!file_name_data.validity.RowIsValid(file_idx)) {
					throw InternalException("Filename cannot be NULL!");
				}
				RecordFile(gstate, state, file_names[file_idx]);
			}
		}
	}
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

const unordered_set<string> &IcebergCopyOnWriteRowCounter::GetFiles() const {
	if (!op_state) {
		throw InternalException("Copy-on-write DELETE: the files were read before they were recorded");
	}
	return op_state->Cast<RowCounterGlobalState>().files;
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
