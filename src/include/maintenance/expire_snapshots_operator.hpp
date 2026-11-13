#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

#include "maintenance/expire_snapshots_planner.hpp"

namespace duckdb {

struct LogicalExpireSnapshots : public LogicalExtensionOperator {
public:
	LogicalExpireSnapshots(idx_t bind_index, ExpireSnapshotsPlanInput input);

	idx_t bind_index;
	ExpireSnapshotsPlanInput input;

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

class PhysicalExpireSnapshots : public PhysicalOperator {
public:
	PhysicalExpireSnapshots(PhysicalPlan &physical_plan, ExpireSnapshotsPlanInput input, idx_t estimated_cardinality);

	ExpireSnapshotsPlanInput input;

	bool IsSource() const override {
		return true;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
