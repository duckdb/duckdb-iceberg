//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/iceberg_file_rewrite.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/operator/iceberg_insert.hpp"
#include "execution/operator/iceberg_written_files.hpp"
#include "catalog/rest/transaction/iceberg_transaction_metadata.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"

namespace duckdb {

struct IcebergTransactionData;

struct IcebergFileRewriteInput {
	explicit IcebergFileRewriteInput(IcebergSnapshotOperationType operation) : operation(operation) {
	}

	//! Data files being replaced, and any delete files whose effects were fully applied.
	vector<IcebergFileIdentity> replaced_files;
	IcebergSnapshotOperationType operation;
	//! Maintenance preserves the starting data sequence; row-changing rewrites inherit the new snapshot's sequence.
	optional<int64_t> data_sequence_number;
};

class IcebergFileRewriteGlobalState : public IcebergInsertGlobalState {
public:
	IcebergFileRewriteGlobalState(ClientContext &context, IcebergTable &table_info);
	~IcebergFileRewriteGlobalState() override;

	IcebergTable &table_info;
	vector<string> produced_paths;
	idx_t added_data_files = 0;
	//! Capture the catalog's cleanup policy while table_info is alive.
	const bool cleanup_enabled;
	//! Once staged, the transaction owns these files, including when publication has not happened yet.
	bool staged = false;
};

//! Writes replacement data files and stages their additions and input-file invalidations together.
//! Callers supply file selection, snapshot semantics and result columns. Maintenance-specific transaction
//! restrictions live in the maintenance caller, not in this shared execution path.
class IcebergFileRewrite : public PhysicalOperator {
public:
	IcebergFileRewrite(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
	                   IcebergWriteOperation write_operation);

	void PlanCopy(ClientContext &context, PhysicalPlanGenerator &planner, const IcebergTableMetadata &metadata,
	              PhysicalOperator &rows, optional_idx target_file_size = optional_idx());

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
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	string GetName() const override;

protected:
	virtual IcebergFileRewriteInput GetRewriteInput(const IcebergFileRewriteGlobalState &state) const = 0;
	virtual void StageRewrite(IcebergTransactionData &transaction_data, const IcebergFileRewriteGlobalState &state,
	                          IcebergSnapshotOperationType operation, vector<IcebergManifestEntry> &&new_entries,
	                          IcebergManifestDeletes &&replaced_files) const;

private:
	const IcebergWriteOperation write_operation;
};

} // namespace duckdb
