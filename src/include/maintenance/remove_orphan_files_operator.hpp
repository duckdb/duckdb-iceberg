#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

struct RemoveOrphanFilesPlanInput {
	QualifiedName table_name;
	bool dry_run = false;
	optional<timestamp_t> older_than;
	string location;
};

struct LogicalRemoveOrphanFiles : public LogicalExtensionOperator {
public:
	LogicalRemoveOrphanFiles(idx_t bind_index, RemoveOrphanFilesPlanInput input);

	idx_t bind_index;
	RemoveOrphanFilesPlanInput input;

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

class PhysicalRemoveOrphanFiles : public PhysicalOperator {
public:
	PhysicalRemoveOrphanFiles(PhysicalPlan &physical_plan, RemoveOrphanFilesPlanInput input,
	                          idx_t estimated_cardinality);

	RemoveOrphanFilesPlanInput input;

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
