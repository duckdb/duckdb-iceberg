#include "maintenance/rewrite_data_files_operator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/operator/set/physical_union.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "maintenance/maintenance_table_loader.hpp"
#include "maintenance/rewrite_data_files_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"

namespace duckdb {

namespace {

static vector<LogicalType> RewriteResultTypes() {
	return {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
}

struct RewriteDataFilesGlobalState : public GlobalSinkState {
	RewriteDataFilesGlobalState(ClientContext &context_p, const RewritePlan &source_plan)
	    : context(context_p), plan(source_plan), group_seen(source_plan.file_groups.size(), false),
	      group_accounted(source_plan.file_groups.size(), false) {
		plan.table_info = ReloadIcebergTableShared(context, plan.table_name, "iceberg_rewrite_data_files");
		ValidateRewriteSnapshot(plan, *plan.table_info, "execution");
	}

	~RewriteDataFilesGlobalState() override {
		if (!committed && plan.table_info) {
			CleanupRewriteFiles(context, *plan.table_info, produced_paths);
		}
	}

	ClientContext &context;
	RewritePlan plan;
	mutex lock;
	vector<bool> group_seen;
	vector<bool> group_accounted;
	vector<string> produced_paths;
	RewriteExecutionResult result;
	bool committed = false;
};

struct RewriteDataFilesGlobalSourceState : public GlobalSourceState {
	RewriteDataFilesGlobalSourceState() = default;

	explicit RewriteDataFilesGlobalSourceState(ClientContext &context, const RewritePlan &source_plan) {
		auto table_info = ReloadIcebergTableShared(context, source_plan.table_name, "iceberg_rewrite_data_files");
		ValidateRewriteSnapshot(source_plan, *table_info, "execution");
	}

	bool emitted = false;
};

struct RewriteDataFilesLocalState : public LocalSinkState {};

} // namespace

LogicalRewriteDataFiles::LogicalRewriteDataFiles(idx_t bind_index_p, RewritePlan plan_p)
    : LogicalExtensionOperator(), bind_index(bind_index_p), plan(std::move(plan_p)) {
}

void LogicalRewriteDataFiles::ResolveTypes() {
	types = RewriteResultTypes();
}

vector<ColumnBinding> LogicalRewriteDataFiles::GetColumnBindings() {
	return GenerateColumnBindings(TableIndex(bind_index), 3);
}

vector<TableIndex> LogicalRewriteDataFiles::GetTableIndex() const {
	return {TableIndex(bind_index)};
}

string LogicalRewriteDataFiles::GetName() const {
	return "ICEBERG_REWRITE_DATA_FILES";
}

PhysicalOperator &LogicalRewriteDataFiles::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &rewrite = planner.Make<PhysicalRewriteDataFiles>(std::move(plan), estimated_cardinality);
	if (children.empty()) {
		return rewrite;
	}

	ArenaLinkedList<reference<PhysicalOperator>> physical_children(planner.ArenaRef());
	for (idx_t group_idx = 0; group_idx < children.size(); group_idx++) {
		auto &child = children[group_idx];
		auto &child_plan = planner.CreatePlan(*child);
		auto child_types = child_plan.GetTypes();

		vector<unique_ptr<Expression>> projection_expressions;
		//! Prefix COPY RETURN_STATS rows so the Sink can map each output file back to its rewrite group.
		projection_expressions.push_back(make_uniq<BoundConstantExpression>(Value::UBIGINT(group_idx)));
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto &child_type = child_types[i];
			projection_expressions.push_back(make_uniq<BoundReferenceExpression>(child_type, i));
		}
		child_types.insert(child_types.begin(), LogicalType::UBIGINT);
		auto &physical_projection = planner.Make<PhysicalProjection>(child_types, std::move(projection_expressions), 1);
		physical_projection.children.push_back(child_plan);
		physical_children.push_back(physical_projection);
	}
	if (physical_children.size() == 1) {
		rewrite.children.push_back(physical_children[0]);
		return rewrite;
	}

	auto &union_op = planner.Make<PhysicalUnion>(physical_children[0].get().GetTypes(), physical_children,
	                                             estimated_cardinality, true);
	rewrite.children.push_back(union_op);
	return rewrite;
}

PhysicalRewriteDataFiles::PhysicalRewriteDataFiles(PhysicalPlan &physical_plan, RewritePlan plan_p,
                                                   idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, RewriteResultTypes(), estimated_cardinality),
      plan(std::move(plan_p)) {
}

void PhysicalRewriteDataFiles::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	if (children.empty()) {
		PhysicalOperator::BuildPipelines(current, meta_pipeline);
		return;
	}
	D_ASSERT(children.size() == 1);

	op_state.reset();
	sink_state = GetGlobalSinkState(current.GetClientContext());
	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);
	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_meta_pipeline.Build(children[0].get());
}

unique_ptr<GlobalSinkState> PhysicalRewriteDataFiles::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RewriteDataFilesGlobalState>(context, plan);
}

unique_ptr<LocalSinkState> PhysicalRewriteDataFiles::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<RewriteDataFilesLocalState>();
}

SinkResultType PhysicalRewriteDataFiles::Sink(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<RewriteDataFilesGlobalState>();
	for (idx_t row = 0; row < chunk.size(); row++) {
		if (chunk.ColumnCount() < 4) {
			throw InternalException(
			    "iceberg_rewrite_data_files: expected COPY RETURN_STATS result with at least four columns");
		}
		auto group_idx = chunk.GetValue(0, row).GetValue<uint64_t>();
		if (static_cast<idx_t>(group_idx) >= gstate.plan.file_groups.size()) {
			throw InternalException("iceberg_rewrite_data_files: COPY returned unknown group index %lld", group_idx);
		}
		//! COPY RETURN_STATS emits one row per produced file:
		//!   0 group_idx
		//!   1 filename
		//!   2 count
		//! followed by file size, footer size, column stats, and partition keys.
		auto produced_file = chunk.GetValue(1, row).GetValue<string>();
		if (produced_file.empty()) {
			throw InternalException("iceberg_rewrite_data_files: COPY for group %lld returned an empty file path",
			                        group_idx);
		}
		auto count = NumericCast<int64_t>(chunk.GetValue(2, row).GetValue<uint64_t>());
		auto file_size_in_bytes = NumericCast<int64_t>(chunk.GetValue(3, row).GetValue<uint64_t>());
		auto &group = gstate.plan.file_groups[group_idx];
		auto entry = BuildRewriteManifestEntry(group, gstate.plan.starting_sequence_number, count, produced_file,
		                                       file_size_in_bytes);

		{
			lock_guard<mutex> guard(gstate.lock);
			gstate.group_seen[group_idx] = true;
			gstate.produced_paths.push_back(produced_file);
			gstate.result.new_entries.push_back(std::move(entry));
			gstate.result.added_data_files++;
			if (!gstate.group_accounted[group_idx]) {
				gstate.group_accounted[group_idx] = true;
				gstate.result.rewritten_data_files += static_cast<int64_t>(group.size());
				for (auto &candidate : group) {
					gstate.result.rewritten_bytes += candidate.file_size_in_bytes;
					gstate.result.rewritten_candidates.push_back(candidate);
				}
			}
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalRewriteDataFiles::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                    OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<RewriteDataFilesGlobalState>();
	{
		lock_guard<mutex> guard(gstate.lock);
		for (idx_t group_idx = 0; group_idx < gstate.group_seen.size(); group_idx++) {
			if (!gstate.group_seen[group_idx]) {
				throw InternalException("iceberg_rewrite_data_files: COPY returned no result for group %llu",
				                        static_cast<unsigned long long>(group_idx));
			}
		}
	}
	CommitRewrite(context, gstate.plan, gstate.result);
	gstate.committed = true;
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> PhysicalRewriteDataFiles::GetGlobalSourceState(ClientContext &context) const {
	if (children.empty()) {
		return make_uniq<RewriteDataFilesGlobalSourceState>(context, plan);
	}
	return make_uniq<RewriteDataFilesGlobalSourceState>();
}

SourceResultType PhysicalRewriteDataFiles::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                           OperatorSourceInput &input) const {
	auto &source_state = input.global_state.Cast<RewriteDataFilesGlobalSourceState>();
	if (source_state.emitted) {
		return SourceResultType::FINISHED;
	}
	source_state.emitted = true;

	int64_t rewritten_data_files = 0;
	int64_t added_data_files = 0;
	int64_t rewritten_bytes = 0;
	if (!children.empty()) {
		auto &gstate = sink_state->Cast<RewriteDataFilesGlobalState>();
		rewritten_data_files = gstate.result.rewritten_data_files;
		added_data_files = gstate.result.added_data_files;
		rewritten_bytes = gstate.result.rewritten_bytes;
	}
	chunk.SetValue(0, 0, Value::BIGINT(rewritten_data_files));
	chunk.SetValue(1, 0, Value::BIGINT(added_data_files));
	chunk.SetValue(2, 0, Value::BIGINT(rewritten_bytes));
	chunk.SetChildCardinality(1);
	return SourceResultType::FINISHED;
}

string PhysicalRewriteDataFiles::GetName() const {
	return "ICEBERG_REWRITE_DATA_FILES";
}

InsertionOrderPreservingMap<string> PhysicalRewriteDataFiles::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Groups"] = std::to_string(plan.file_groups.size());
	return result;
}

} // namespace duckdb
