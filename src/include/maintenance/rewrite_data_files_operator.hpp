#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

#include "execution/operator/iceberg_file_rewrite.hpp"
#include "maintenance/rewrite_data_files_planner.hpp"

namespace duckdb {

struct LogicalRewriteDataFiles : public LogicalExtensionOperator {
public:
	explicit LogicalRewriteDataFiles(idx_t bind_index, RewritePlan plan);

	idx_t bind_index;
	RewritePlan plan;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	vector<ColumnBinding> GetColumnBindings() override;
	vector<TableIndex> GetTableIndex() const override;
	string GetName() const override;
	bool SupportSerialization() const override {
		return false;
	}

protected:
	void ResolveTypes() override;
};

class PhysicalRewriteDataFiles : public IcebergFileRewrite {
public:
	PhysicalRewriteDataFiles(PhysicalPlan &physical_plan, RewritePlan plan, idx_t estimated_cardinality);

	RewritePlan plan;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

protected:
	IcebergFileRewriteInput GetRewriteInput(const IcebergFileRewriteGlobalState &state) const override;
	void StageRewrite(IcebergTransactionData &transaction_data, const IcebergFileRewriteGlobalState &state,
	                  IcebergSnapshotOperationType operation, vector<IcebergManifestEntry> &&new_entries,
	                  IcebergManifestDeletes &&replaced_files) const override;
};

} // namespace duckdb
