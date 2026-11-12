#include "maintenance/rewrite_data_files_operator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "maintenance/maintenance_table_loader.hpp"
#include "maintenance/rewrite_data_files_executor.hpp"

namespace duckdb {

namespace {

static vector<LogicalType> RewriteResultTypes() {
	return {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
}

struct RewriteDataFilesGlobalState : public IcebergFileRewriteGlobalState {
	RewriteDataFilesGlobalState(ClientContext &context, RewritePlan plan_p)
	    : IcebergFileRewriteGlobalState(context, *plan_p.table_info), plan(std::move(plan_p)) {
	}

	RewritePlan plan;
};

struct RewriteDataFilesGlobalSourceState : public GlobalSourceState {
	RewriteDataFilesGlobalSourceState() = default;

	explicit RewriteDataFilesGlobalSourceState(ClientContext &context, const RewritePlan &source_plan) {
		auto table_info = ReloadIcebergTableShared(context, source_plan.table_name, "iceberg_rewrite_data_files");
		ValidateRewriteSnapshot(source_plan, *table_info, "execution");
	}

	bool emitted = false;
};

} // namespace

LogicalRewriteDataFiles::LogicalRewriteDataFiles(idx_t bind_index_p, RewritePlan plan_p)
    : LogicalExtensionOperator(), bind_index(bind_index_p), plan(std::move(plan_p)) {
}

void LogicalRewriteDataFiles::ResolveTypes() {
	types = RewriteResultTypes();
}

vector<ColumnBinding> LogicalRewriteDataFiles::GetColumnBindings() {
	return GenerateColumnBindings(TableIndex(bind_index), 5);
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

	//! Bind attached a LogicalCopyToFile so RemoveUnusedColumns keeps all table
	//! columns. Peel that logical COPY away and rebuild the physical copy with
	//! the shared Iceberg file-rewrite writer (partitioned IcebergCopyOptions).
	auto &logical_child = *children[0];
	if (logical_child.type != LogicalOperatorType::LOGICAL_COPY_TO_FILE || logical_child.children.size() != 1) {
		throw InternalException(
		    "iceberg_rewrite_data_files: expected a single LogicalCopyToFile child with one scan child");
	}
	auto &scan = planner.CreatePlan(*logical_child.children[0]);
	//! Vended credentials are already installed: PlanRewrite loads them for the manifests,
	//! and BindCandidateCopy's table scan bind calls PrepareIcebergScanFromEntry.
	rewrite.PlanCopy(context, planner, metadata, scan, NumericCast<idx_t>(rewrite.plan.target_file_size_bytes));
	return rewrite;
}

PhysicalRewriteDataFiles::PhysicalRewriteDataFiles(PhysicalPlan &physical_plan, RewritePlan plan_p,
                                                   idx_t estimated_cardinality)
    : IcebergFileRewrite(physical_plan, RewriteResultTypes(), estimated_cardinality), plan(std::move(plan_p)) {
}

unique_ptr<GlobalSinkState> PhysicalRewriteDataFiles::GetGlobalSinkState(ClientContext &context) const {
	auto execution_plan = plan;
	execution_plan.table_info = ReloadIcebergTableShared(context, plan.table_name, "iceberg_rewrite_data_files");
	ValidateRewriteSnapshot(execution_plan, *execution_plan.table_info, "execution");
	return make_uniq<RewriteDataFilesGlobalState>(context, std::move(execution_plan));
}

IcebergFileRewriteInput PhysicalRewriteDataFiles::GetRewriteInput(const IcebergFileRewriteGlobalState &state) const {
	auto &plan = state.Cast<RewriteDataFilesGlobalState>().plan;
	IcebergFileRewriteInput result(IcebergSnapshotOperationType::REPLACE);
	result.data_sequence_number = plan.starting_sequence_number;
	for (auto &candidate : plan.selected_candidates) {
		result.replaced_files.emplace_back(candidate.file_path);
	}
	for (auto &path : plan.delete_files_to_remove) {
		result.replaced_files.emplace_back(path);
	}
	return result;
}

void PhysicalRewriteDataFiles::StageRewrite(IcebergTransactionData &transaction_data,
                                            const IcebergFileRewriteGlobalState &state,
                                            IcebergSnapshotOperationType operation,
                                            vector<IcebergManifestEntry> &&new_entries,
                                            IcebergManifestDeletes &&replaced_files) const {
	auto &plan = state.Cast<RewriteDataFilesGlobalState>().plan;
	//! Only replace the selected files after verifying that their live rows were preserved.
	ValidateRewrittenRowCount(plan, new_entries);
	ValidateRewriteSnapshot(plan, state.table_info, "commit");
	ValidateRewriteSnapshot(plan, transaction_data.table_info, "transaction commit");
	{
		lock_guard<mutex> guard(transaction_data.lock);
		transaction_data.VerifyCanAddSnapshot();
		//! Recheck at execution: other operators in this statement may have staged changes since binding.
		ValidateRewriteTransaction(transaction_data);
	}
	IcebergFileRewrite::StageRewrite(transaction_data, state, operation, std::move(new_entries),
	                                 std::move(replaced_files));
	//! This is a maintenance restriction, not an implicit side effect of writing a REPLACE snapshot.
	lock_guard<mutex> guard(transaction_data.lock);
	transaction_data.has_maintenance_rewrite = true;
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
	int64_t removed_delete_files = 0;
	if (!children.empty()) {
		auto &gstate = sink_state->Cast<RewriteDataFilesGlobalState>();
		rewritten_data_files = NumericCast<int64_t>(gstate.plan.selected_candidates.size());
		added_data_files = NumericCast<int64_t>(gstate.added_data_files);
		for (auto &candidate : gstate.plan.selected_candidates) {
			rewritten_bytes += candidate.file_size_in_bytes;
		}
		removed_delete_files = NumericCast<int64_t>(gstate.plan.delete_files_to_remove.size());
	}
	chunk.data[0].Append(Value::BIGINT(rewritten_data_files));
	chunk.data[1].Append(Value::BIGINT(added_data_files));
	chunk.data[2].Append(Value::BIGINT(rewritten_bytes));
	chunk.data[3].Append(Value::BIGINT(0));
	chunk.data[4].Append(Value::BIGINT(removed_delete_files));
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
