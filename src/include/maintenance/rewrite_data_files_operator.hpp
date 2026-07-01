#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

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

class PhysicalRewriteDataFiles : public PhysicalOperator {
public:
	PhysicalRewriteDataFiles(PhysicalPlan &physical_plan, RewritePlan plan, idx_t estimated_cardinality);

	RewritePlan plan;

	bool IsSink() const override {
		return !children.empty();
	}
	bool ParallelSink() const override {
		return true;
	}
	bool IsSource() const override {
		return true;
	}

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
