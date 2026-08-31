#include "maintenance/rewrite_data_files_operator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "execution/operator/iceberg_insert.hpp"
#include "maintenance/maintenance_table_loader.hpp"
#include "maintenance/rewrite_data_files_executor.hpp"

namespace duckdb {

namespace {

static vector<LogicalType> RewriteResultTypes() {
	return {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
}

struct RewriteDataFilesGlobalState : public GlobalSinkState {
	RewriteDataFilesGlobalState(ClientContext &context_p, const RewritePlan &source_plan)
	    : context(context_p), plan(source_plan), insert_state(context_p) {
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
	IcebergInsertGlobalState insert_state;
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
	auto &rewrite =
	    planner.Make<PhysicalRewriteDataFiles>(std::move(plan), estimated_cardinality).Cast<PhysicalRewriteDataFiles>();
	if (children.empty()) {
		return rewrite;
	}

	D_ASSERT(children.size() == 1);
	D_ASSERT(rewrite.plan.table_info);
	auto &metadata = rewrite.plan.table_info->table_metadata;
	auto schema_id = metadata.GetCurrentSchemaId();
	auto schema_it = metadata.GetSchemas().find(schema_id);
	if (schema_it == metadata.GetSchemas().end()) {
		throw InternalException("iceberg_rewrite_data_files: current schema id %d not found in metadata", schema_id);
	}

	//! Bind attached a LogicalCopyToFile so RemoveUnusedColumns keeps all table
	//! columns. Peel that logical COPY away and rebuild the physical copy with
	//! IcebergInsert::PlanCopyForInsert (partitioned IcebergCopyOptions).
	auto &logical_child = *children[0];
	if (logical_child.type != LogicalOperatorType::LOGICAL_COPY_TO_FILE || logical_child.children.size() != 1) {
		throw InternalException(
		    "iceberg_rewrite_data_files: expected a single LogicalCopyToFile child with one scan child");
	}
	auto &scan = planner.CreatePlan(*logical_child.children[0]);
	//! Vended credentials are already installed: PlanRewrite loads them for the manifests,
	//! and BindCandidateCopy's table scan bind calls PrepareIcebergScanFromEntry.
	IcebergCopyInput copy_input(context, metadata, *schema_it->second);
	auto &copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, &scan).Cast<PhysicalCopyToFile>();
	copy.file_size_bytes = NumericCast<idx_t>(rewrite.plan.target_file_size_bytes);
	//! A file can never be smaller than a single row group; rotation only happens at row-group
	//! boundaries. Cap batch_size_bytes to the rewrite target so FILE_SIZE_BYTES rotation remains
	//! effective (mirrors IcebergInsert::GetCopyOptions).
	if (copy.batch_size_bytes.IsValid() && copy.file_size_bytes.IsValid() &&
	    copy.batch_size_bytes.GetIndex() > copy.file_size_bytes.GetIndex()) {
		copy.batch_size_bytes = copy.file_size_bytes;
	}
	rewrite.children.push_back(copy);
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
	auto &table_info = *gstate.plan.table_info;
	auto table_name = gstate.plan.table_name.Name().GetIdentifierName();

	{
		lock_guard<mutex> guard(gstate.lock);
		gstate.insert_state.AddFiles(chunk, table_name, table_info.table_metadata);
		for (idx_t row = 0; row < chunk.size(); row++) {
			auto produced_file = chunk.GetValue(0, row).GetValue<string>();
			if (produced_file.empty()) {
				throw InternalException("iceberg_rewrite_data_files: COPY returned an empty file path");
			}
			gstate.produced_paths.push_back(produced_file);
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalRewriteDataFiles::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                    OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<RewriteDataFilesGlobalState>();
	{
		lock_guard<mutex> guard(gstate.lock);
		if (gstate.insert_state.written_files.empty()) {
			throw InternalException(
			    "iceberg_rewrite_data_files: COPY returned no written files for selected candidates");
		}
		gstate.result.new_entries = IcebergInsert::GetInsertManifestEntries(gstate.insert_state);
		PinRewriteSequenceNumbers(gstate.result.new_entries, gstate.plan.starting_sequence_number);
		gstate.result.added_data_files = static_cast<int64_t>(gstate.result.new_entries.size());
		AccountSelectedCandidates(gstate.plan, gstate.result);
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
	chunk.data[0].Append(Value::BIGINT(rewritten_data_files));
	chunk.data[1].Append(Value::BIGINT(added_data_files));
	chunk.data[2].Append(Value::BIGINT(rewritten_bytes));
	return SourceResultType::FINISHED;
}

string PhysicalRewriteDataFiles::GetName() const {
	return "ICEBERG_REWRITE_DATA_FILES";
}

InsertionOrderPreservingMap<string> PhysicalRewriteDataFiles::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Selected Files"] = std::to_string(plan.selected_candidates.size());
	return result;
}

} // namespace duckdb
