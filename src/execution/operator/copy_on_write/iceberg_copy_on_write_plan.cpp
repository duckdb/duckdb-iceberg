#include "execution/operator/copy_on_write/iceberg_copy_on_write.hpp"
#include "execution/operator/copy_on_write/iceberg_copy_on_write_operators.hpp"

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
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include "duckdb/planner/operator/logical_set_operation.hpp"

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

//! The body of the `raw` CTE: the DELETE's own scan, reduced to the (filename, file_row_number) of each row it matches.
//! DELETE ... USING can match a row more than once; the anti join does not need the pairs to be distinct. The DELETE's
//! plan is already optimized and resolved, so it is planned as is rather than as a child of this tree.
class LogicalCopyOnWriteDeleteMatches : public LogicalExtensionOperator {
public:
	LogicalCopyOnWriteDeleteMatches(LogicalOperator &match_plan, vector<idx_t> row_id_indexes,
	                                vector<LogicalType> row_id_types, vector<unique_ptr<Expression>> values)
	    : match_plan(match_plan), row_id_indexes(std::move(row_id_indexes)), row_id_types(std::move(row_id_types)),
	      values(std::move(values)) {
		types = GetRawTypes();
	}

	LogicalOperator &match_plan;
	//! The filename and file_row_number columns of match_plan, and their types
	vector<idx_t> row_id_indexes;
	vector<LogicalType> row_id_types;
	//! For an UPDATE: the new version of the row, evaluated on match_plan's output
	vector<unique_ptr<Expression>> values;

	vector<LogicalType> GetRawTypes() const {
		auto result = row_id_types;
		for (auto &value : values) {
			result.push_back(value->GetReturnType());
		}
		return result;
	}

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &match = planner.CreatePlan(match_plan);
		vector<unique_ptr<Expression>> select_list;
		for (idx_t i = 0; i < row_id_indexes.size(); i++) {
			select_list.push_back(make_uniq<BoundReferenceExpression>(row_id_types[i], row_id_indexes[i]));
		}
		for (auto &value : values) {
			select_list.push_back(value->Copy());
		}
		auto &projection =
		    planner.Make<PhysicalProjection>(GetRawTypes(), std::move(select_list), match.estimated_cardinality);
		projection.children.push_back(match);
		return projection;
	}

	string GetExtensionName() const override {
		return "iceberg_copy_on_write_delete_matches";
	}

protected:
	void ResolveTypes() override {
		types = GetRawTypes();
	}
};

class LogicalCopyOnWriteMergePositions : public LogicalExtensionOperator {
public:
	LogicalCopyOnWriteMergePositions(PhysicalOperator &merge_plan, shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows,
	                                 shared_ptr<IcebergCopyOnWriteMergeResult> result)
	    : merge_plan(merge_plan), merge_rows(std::move(merge_rows)), result(std::move(result)) {
		types = {LogicalType::VARCHAR, LogicalType::BIGINT};
	}

	PhysicalOperator &merge_plan;
	shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows;
	shared_ptr<IcebergCopyOnWriteMergeResult> result;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		return planner.Make<IcebergCopyOnWriteMergePositions>(merge_plan, merge_rows, result);
	}

	string GetExtensionName() const override {
		return "iceberg_copy_on_write_merge_positions";
	}

protected:
	void ResolveTypes() override {
		types = {LogicalType::VARCHAR, LogicalType::BIGINT};
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

//! See IcebergCopyOnWriteLineageCheck; its output is its child's
class LogicalCopyOnWriteLineageCheck : public LogicalExtensionOperator {
public:
	LogicalCopyOnWriteLineageCheck(unique_ptr<LogicalOperator> child, string statement)
	    : statement(std::move(statement)) {
		children.push_back(std::move(child));
	}

	string statement;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);
		return planner.Make<IcebergCopyOnWriteLineageCheck>(child, statement);
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

	string GetExtensionName() const override {
		return "iceberg_copy_on_write_lineage_check";
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};

//! Plans a copy-on-write DELETE (values empty) or UPDATE (values: the new version of each matched row, evaluated on
//! match_plan's output), see IcebergCopyOnWriteDelete
PhysicalOperator &PlanJoinRewrite(ClientContext &context, PhysicalPlanGenerator &planner,
                                  IcebergTableSchemaVersion &table, LogicalOperator &match_plan,
                                  vector<idx_t> row_id_indexes, vector<LogicalType> row_id_types,
                                  vector<unique_ptr<Expression>> values,
                                  optional_ptr<PhysicalOperator> merge_plan = nullptr,
                                  shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows = nullptr);

} // namespace

PhysicalOperator &IcebergCopyOnWriteDelete::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                       LogicalDelete &op, IcebergTableSchemaVersion &table) {
	//! The DELETE's own scan emits the filename and file_row_number of every matching row
	vector<idx_t> row_id_indexes;
	vector<LogicalType> row_id_types;
	for (idx_t i = 0; i < 2; i++) {
		auto &ref = op.expressions[i]->Cast<BoundReferenceExpression>();
		row_id_indexes.push_back(ref.Index());
		row_id_types.push_back(ref.GetReturnType());
	}
	return PlanJoinRewrite(context, planner, table, *op.children[0], std::move(row_id_indexes), std::move(row_id_types),
	                       {});
}

PhysicalOperator &IcebergCopyOnWriteDelete::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                       LogicalUpdate &op, IcebergTableSchemaVersion &table) {
	//! An Iceberg UPDATE is bound as a delete and an insert, so its expressions give the new value of every table
	//! column, followed in the scan's output by filename and file_row_number and, on V3, _row_id (see
	//! IcebergTableSchemaVersion::GetRowIdColumns).
	auto &match_plan = *op.children[0];
	const bool with_row_lineage = table.table_info.GetMetadata().iceberg_version >= 3;
	auto match_column_count = match_plan.types.size();
	const idx_t lineage_column_count = with_row_lineage ? 1 : 0;
	D_ASSERT(match_column_count >= 2 + lineage_column_count);
	vector<idx_t> row_id_indexes {match_column_count - lineage_column_count - 2,
	                              match_column_count - lineage_column_count - 1};
	vector<LogicalType> row_id_types {match_plan.types[row_id_indexes[0]], match_plan.types[row_id_indexes[1]]};

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
	vector<unique_ptr<Expression>> values;
	for (idx_t column = 0; column < op.columns.size(); column++) {
		auto expression_index = expression_index_by_column.at(column);
		auto expression = op.expressions[expression_index]->Copy();
		if (expression->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			expression = op.bound_defaults[expression_index]->Copy();
		}
		values.push_back(std::move(expression));
	}
	if (with_row_lineage) {
		//! SPEC: an updated row keeps its _row_id, and its _last_updated_sequence_number is written as NULL, so it
		//! inherits the sequence number of the snapshot that commits the update. A row inserted earlier in this
		//! transaction has no _row_id yet; it gets one at commit, as with a merge-on-read UPDATE.
		auto row_id_index = match_column_count - 1;
		values.push_back(make_uniq<BoundReferenceExpression>(match_plan.types[row_id_index], row_id_index));
		values.push_back(make_uniq<BoundConstantExpression>(Value(LogicalType::BIGINT)));
	}
	return PlanJoinRewrite(context, planner, table, match_plan, std::move(row_id_indexes), std::move(row_id_types),
	                       std::move(values));
}

PhysicalOperator &IcebergCopyOnWriteDelete::PlanMerge(ClientContext &context, PhysicalPlanGenerator &planner,
                                                      LogicalMergeInto &op, PhysicalOperator &merge_plan,
                                                      IcebergTableSchemaVersion &table,
                                                      shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows) {
	//! raw: the positions the MERGE's UPDATE and DELETE actions removed, once it has run
	return PlanJoinRewrite(context, planner, table, op, {}, {LogicalType::VARCHAR, LogicalType::BIGINT}, {}, merge_plan,
	                       std::move(merge_rows));
}

namespace {

PhysicalOperator &PlanJoinRewrite(ClientContext &context, PhysicalPlanGenerator &planner,
                                  IcebergTableSchemaVersion &table, LogicalOperator &match_plan,
                                  vector<idx_t> row_id_indexes, vector<LogicalType> row_id_types,
                                  vector<unique_ptr<Expression>> values, optional_ptr<PhysicalOperator> merge_plan,
                                  shared_ptr<IcebergCopyOnWriteMergeRows> merge_rows) {
	auto &metadata = table.table_info.GetMetadata();
	//! A V3 rewrite carries each kept row's _row_id and _last_updated_sequence_number into the new files.
	//! Rows inserted in this transaction carry NULL until the commit assigns their lineage.
	const bool with_row_lineage = metadata.iceberg_version >= 3;
	const bool is_update = !values.empty();
	const string statement = merge_plan ? "MERGE" : is_update ? "UPDATE" : "DELETE";

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

	//! raw: the matched rows' filename and file_row_number, and for an UPDATE their new version, with the types of
	//! the rows the rewrite writes
	D_ASSERT(!is_update || values.size() == column_count);
	for (idx_t i = 0; i < values.size(); i++) {
		if (values[i]->GetReturnType() != column_types[i]) {
			values[i] = BoundCastExpression::AddCastToType(context, std::move(values[i]), column_types[i]);
		}
	}
	//! For a MERGE, raw is the positions its actions removed; match_plan is the MERGE's logical plan, used only for
	//! its table indexes
	unique_ptr<LogicalOperator> matches;
	shared_ptr<IcebergCopyOnWriteMergeResult> merge_result;
	if (merge_plan) {
		merge_result = make_shared_ptr<IcebergCopyOnWriteMergeResult>();
		matches = make_uniq<LogicalCopyOnWriteMergePositions>(*merge_plan, std::move(merge_rows), merge_result);
	} else {
		matches = make_uniq<LogicalCopyOnWriteDeleteMatches>(match_plan, std::move(row_id_indexes), row_id_types,
		                                                     std::move(values));
	}
	auto raw_types = matches->types;
	vector<Identifier> raw_names {"filename", "file_row_number"};
	for (idx_t i = 2; i < raw_types.size(); i++) {
		raw_names.emplace_back("value" + to_string(i - 2));
	}

	//! The CTE indexes have to be distinct from any CTE in the statement's plan: they share the physical planner
	idx_t max_index = 0;
	MaxTableIndex(match_plan, max_index);
	MaxTableIndex(*keep, max_index);
	TableIndex raw_index(MaxValue(max_index + 1, binder->GenerateTableIndex().index));
	TableIndex dict_index(raw_index.index + 1);
	auto raw_ref = [&]() {
		return make_uniq<LogicalCTERef>(binder->GenerateTableIndex(), raw_index, raw_types, raw_names);
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
	//! An earlier V3 snapshot in this transaction will assign any lineage that still reads NULL. Only the kept rows
	//! must have theirs: an updated row's sequence number is NULL on purpose.
	auto &transaction_data = table.table_info.transaction_data;
	if (with_row_lineage && transaction_data && transaction_data->has_pending_v3_snapshot) {
		rows = make_uniq<LogicalCopyOnWriteLineageCheck>(std::move(rows), statement);
	}

	//! For an UPDATE: UNION ALL SELECT <new values> FROM raw, counted
	optional_ptr<LogicalCopyOnWriteRowCount> updated_rows_ref;
	if (is_update) {
		auto updated_raw = raw_ref();
		auto updated_raw_index = updated_raw->table_index;
		vector<unique_ptr<Expression>> updated_list;
		for (idx_t i = 0; i < column_count; i++) {
			updated_list.push_back(ColumnRef(raw_types[2 + i], updated_raw_index, 2 + i));
		}
		auto updated = make_uniq<LogicalProjection>(binder->GenerateTableIndex(), std::move(updated_list));
		updated->AddChild(std::move(updated_raw));
		auto updated_rows = make_uniq<LogicalCopyOnWriteRowCount>(std::move(updated));
		updated_rows_ref = updated_rows.get();
		rows = make_uniq<LogicalSetOperation>(binder->GenerateTableIndex(), column_count, std::move(rows),
		                                      std::move(updated_rows), LogicalOperatorType::LOGICAL_UNION, true);
	}

	//! The join with dict restricts the keep scan to the touched files at runtime: its build side becomes a filter on
	//! the constant `filename` column, which the scan evaluates once per file, skipping every other file unread.
	Optimizer optimizer(*binder, context);
	JoinFilterPushdownOptimizer join_filter_pushdown(optimizer);
	join_filter_pushdown.VisitOperator(*rows);
	if (touched_join_ref.filter_pushdown) {
		//! The statement's predicate usually selects few rows, so the touched files are worth filtering on
		touched_join_ref.filter_pushdown->build_side_has_filter = true;
	}

	//! The statement's plan is resolved already; only the new operators above the raw CTE body need it.
	ColumnBindingResolver dict_resolver;
	dict_resolver.VisitOperator(*dict);
	ColumnBindingResolver rows_resolver;
	rows_resolver.VisitOperator(*rows);

	auto with_dict = make_uniq<LogicalMaterializedCTE>("dict", dict_index, 2, std::move(dict), std::move(rows),
	                                                   CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	unique_ptr<LogicalOperator> plan =
	    make_uniq<LogicalMaterializedCTE>("raw", raw_index, raw_types.size(), std::move(matches), std::move(with_dict),
	                                      CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	plan->ResolveOperatorTypes();
	auto &rows_plan = planner.CreatePlan(*plan);
	D_ASSERT(rows_plan.GetTypes().size() == column_count);
	if (!touched_rows_ref.counter || (updated_rows_ref && !updated_rows_ref->counter)) {
		throw InternalException("Copy-on-write %s: the rows are not counted", statement);
	}

	//! Use the same replacement-file writer and snapshot staging as maintenance rewrites.
	auto &rewrite = planner
	                    .Make<IcebergCopyOnWriteRewrite>(table.table_info, *touched_rows_ref.counter,
	                                                     updated_rows_ref ? updated_rows_ref->counter : nullptr,
	                                                     merge_result, target_files, std::move(plan))
	                    .Cast<IcebergCopyOnWriteRewrite>();
	rewrite.PlanCopy(context, planner, metadata, rows_plan, optional_idx(),
	                 with_row_lineage ? IcebergInsertVirtualColumns::WRITE_ROW_ID_AND_SEQUENCE_NUMBER
	                                  : IcebergInsertVirtualColumns::NONE);
	return rewrite;
}

} // namespace

} // namespace duckdb
