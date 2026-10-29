#include "execution/operator/iceberg_copy_on_write_delete.hpp"

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "execution/operator/iceberg_insert.hpp"

namespace duckdb {

namespace {

class CopyOnWriteDeleteGlobalState : public GlobalSinkState {
public:
	mutex lock;
	IcebergCopyOnWriteDeletes deletes;
};

class CopyOnWriteDeleteLocalState : public LocalSinkState {
public:
	unordered_map<string, vector<int64_t>> rows_by_file;
};

class CopyOnWriteDeleteOperatorState : public OperatorState {
public:
	explicit CopyOnWriteDeleteOperatorState() : keep(STANDARD_VECTOR_SIZE) {
	}

	SelectionVector keep;
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

//! Bind `SELECT * FROM <table>`, then add the filename and file_row_number virtual columns to its scan. They are
//! added by column id, not by name, so a table column that happens to be called `filename` cannot shadow them.
unique_ptr<LogicalOperator> BindKeepScan(ClientContext &context, IcebergTableSchemaVersion &table) {
	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(make_uniq<StarExpression>());
	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->SetQualifiedName(table.ParentCatalog().GetName(), table.ParentSchema().name, table.name);
	select->from_table = std::move(table_ref);

	auto binder = Binder::CreateBinder(context);
	auto bound = binder->Bind(*select);
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

} // namespace

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

PhysicalOperator &IcebergCopyOnWriteDeleteFilter::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                             LogicalDelete &op, IcebergTableSchemaVersion &table) {
	auto &metadata = table.table_info.table_metadata;
	if (metadata.iceberg_version >= 3) {
		//! A V3 rewrite has to carry each kept row's _row_id and _last_updated_sequence_number into the new files
		throw NotImplementedException("Copy-on-write DELETE on Iceberg V%d tables", metadata.iceberg_version);
	}

	//! Build side: the DELETE's own scan, which emits the filename and file_row_number of every matching row
	auto &match_plan = planner.CreatePlan(*op.children[0]);
	vector<idx_t> match_row_id_indexes;
	for (idx_t i = 0; i < 2; i++) {
		match_row_id_indexes.push_back(op.expressions[i]->Cast<BoundReferenceExpression>().Index());
	}

	//! Probe side: the whole table again, so the rows of each matched file that survive can be rewritten. It is
	//! planned here, after optimization, like PhysicalPlanGenerator::Plan does for a full statement.
	auto keep_logical_plan = BindKeepScan(context, table);
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*keep_logical_plan);
	keep_logical_plan->ResolveOperatorTypes();
	auto &keep_plan = planner.CreatePlan(*keep_logical_plan);

	auto keep_types = keep_plan.GetTypes();
	D_ASSERT(keep_types.size() > 2);
	vector<LogicalType> output_types(keep_types.begin(), keep_types.end() - 2);

	auto &filter = planner
	                   .Make<IcebergCopyOnWriteDeleteFilter>(std::move(output_types), keep_plan, match_plan,
	                                                         std::move(match_row_id_indexes))
	                   .Cast<IcebergCopyOnWriteDeleteFilter>();
	filter.keep_logical_plan = std::move(keep_logical_plan);

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

	//! The kept rows are written like an INSERT; the insert then swaps the matched files for the new ones
	auto &schema = metadata.GetLatestSchema();
	IcebergCopyInput copy_input(context, metadata, schema);
	auto &copy_op = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, &filter);
	auto &insert_op = IcebergInsert::PlanInsert(context, planner, table).Cast<IcebergInsert>();
	insert_op.copy_on_write_delete_op = filter;
	insert_op.children.push_back(copy_op);
	return insert_op;
}

//===--------------------------------------------------------------------===//
// Sink (build side)
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> IcebergCopyOnWriteDeleteFilter::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<CopyOnWriteDeleteGlobalState>();
}

unique_ptr<LocalSinkState> IcebergCopyOnWriteDeleteFilter::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<CopyOnWriteDeleteLocalState>();
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
			if (!deleted.insert(row_number).second) {
				throw InternalException("Copy-on-write DELETE matched the same row twice");
			}
		}
		global_state.deletes.deleted_count += entry.second.size();
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

	//! The probe side emits the table's columns followed by filename and file_row_number
	auto column_count = types.size();
	D_ASSERT(input.ColumnCount() == column_count + 2);

	UnifiedVectorFormat file_name_data;
	input.data[column_count].ToUnifiedFormat(file_name_data);
	auto file_names = UnifiedVectorFormat::GetData<string_t>(file_name_data);
	UnifiedVectorFormat row_number_data;
	input.data[column_count + 1].ToUnifiedFormat(row_number_data);
	auto row_numbers = UnifiedVectorFormat::GetData<int64_t>(row_number_data);

	idx_t kept = 0;
	for (idx_t i = 0; i < input.size(); i++) {
		auto file_name = file_names[file_name_data.sel->get_index(i)];
		if (!state.has_last_file || !(file_name == string_t(state.last_file))) {
			state.has_last_file = true;
			state.last_file = file_name.GetString();
			auto entry = rows_by_file.find(state.last_file);
			state.last_deleted = entry == rows_by_file.end() ? nullptr : &entry->second;
		}
		if (!state.last_deleted) {
			//! The file has no matching row, so it stays as it is and none of its rows are rewritten
			continue;
		}
		auto row_number = row_numbers[row_number_data.sel->get_index(i)];
		if (state.last_deleted->count(row_number)) {
			continue;
		}
		state.keep.set_index(kept++, i);
	}

	for (idx_t col = 0; col < column_count; col++) {
		chunk.data[col].Slice(input.data[col], state.keep, kept);
	}
	chunk.SetChildCardinality(kept);
	return OperatorResultType::NEED_MORE_INPUT;
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
	return "ICEBERG_COPY_ON_WRITE_DELETE";
}

} // namespace duckdb
