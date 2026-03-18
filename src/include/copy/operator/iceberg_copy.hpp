#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "metadata/iceberg_table_metadata.hpp"
#include "metadata/iceberg_table_schema.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "metadata/iceberg_manifest.hpp"

namespace duckdb {

struct CopyIcebergLocalState : public LocalSinkState {
public:
	explicit CopyIcebergLocalState(ClientContext &context);
};

struct IcebergPhysicalCopy : public PhysicalOperator {
public:
	IcebergPhysicalCopy(PhysicalPlan &plan, vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality) {
	}

	// Keep bind data alive throughout execution (contains metadata and schema)
	unique_ptr<FunctionData> bind_data;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

struct IcebergLogicalCopy : public LogicalExtensionOperator {
public:
	IcebergLogicalCopy() : LogicalExtensionOperator() {
	}

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner);
	void ResolveTypes();

public:
	unique_ptr<FunctionData> bind_data;
};

} // namespace duckdb
