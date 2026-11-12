#include "execution/operator/iceberg_file_rewrite.hpp"

#include "duckdb/logging/logger.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

IcebergFileRewriteGlobalState::IcebergFileRewriteGlobalState(ClientContext &context, IcebergTable &table_info)
    : IcebergInsertGlobalState(context), table_info(table_info),
      cleanup_enabled(table_info.catalog.attach_options.remove_files_on_delete) {
}

IcebergFileRewriteGlobalState::~IcebergFileRewriteGlobalState() {
	if (staged || !cleanup_enabled || produced_paths.empty()) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &path : produced_paths) {
		try {
			fs.TryRemoveFile(path);
		} catch (...) {
			DUCKDB_LOG_DEBUG(context, "Failed to clean up rewrite output file '%s'", path);
		}
	}
}

IcebergFileRewrite::IcebergFileRewrite(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                       idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality) {
}

void IcebergFileRewrite::PlanCopy(ClientContext &context, PhysicalPlanGenerator &planner,
                                  const IcebergTableMetadata &metadata, PhysicalOperator &rows,
                                  optional_idx target_file_size) {
	IcebergCopyInput copy_input(context, metadata, metadata.GetLatestSchema());
	auto &copy = IcebergInsert::PlanCopyForInsert(context, planner, copy_input, &rows);
	if (target_file_size.IsValid()) {
		copy.file_size_bytes = target_file_size;
		//! Files rotate at row-group boundaries, so a row group must not exceed the requested target.
		if (copy.batch_size_bytes.IsValid() && copy.batch_size_bytes.GetIndex() > target_file_size.GetIndex()) {
			copy.batch_size_bytes = target_file_size;
		}
	}
	children.push_back(copy);
}

void IcebergFileRewrite::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	if (children.empty()) {
		PhysicalOperator::BuildPipelines(current, meta_pipeline);
		return;
	}
	D_ASSERT(children.size() == 1);

	op_state.reset();
	//! Initialize before building the COPY pipeline so callers can validate the table before writing files.
	sink_state = GetGlobalSinkState(current.GetClientContext());
	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);
	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_meta_pipeline.Build(children[0].get());
}

SinkResultType IcebergFileRewrite::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.global_state.Cast<IcebergFileRewriteGlobalState>();
	{
		lock_guard<mutex> guard(state.lock);
		//! Record all produced paths before interpreting statistics, which can itself fail.
		for (idx_t row = 0; row < chunk.size(); row++) {
			auto path = chunk.GetValue(0, row).GetValue<string>();
			if (path.empty()) {
				throw InternalException("Iceberg file rewrite: COPY returned an empty file path");
			}
			state.produced_paths.push_back(std::move(path));
		}
	}
	state.AddFiles(chunk, state.table_info.name, state.table_info.GetMetadata());
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType IcebergFileRewrite::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<IcebergFileRewriteGlobalState>();
	auto new_entries = IcebergInsert::GetInsertManifestEntries(state);
	state.added_data_files = new_entries.size();
	auto rewrite = GetRewriteInput(state);
	if (rewrite.replaced_files.empty()) {
		if (!new_entries.empty()) {
			throw InternalException("Iceberg file rewrite produced files without replacing any input files");
		}
		state.staged = true;
		return SinkFinalizeType::READY;
	}

	if (rewrite.data_sequence_number) {
		for (auto &entry : new_entries) {
			entry.SetSequenceNumber(*rewrite.data_sequence_number);
		}
	}
	IcebergManifestDeletes replaced_files;
	for (auto &file : rewrite.replaced_files) {
		replaced_files.InvalidateFile(file);
	}

	auto &transaction = IcebergTransaction::Get(context, state.table_info.catalog);
	//! Multiple rewrite operators in one statement can initialize and update table state concurrently.
	lock_guard<mutex> guard(transaction.rewrite_commit_lock);
	ApplyTableUpdate(state.table_info, transaction, [&](IcebergTable &table) {
		auto &transaction_data = table.GetOrCreateTransactionData(transaction);
		StageRewrite(transaction_data, state, rewrite.operation, std::move(new_entries), std::move(replaced_files));
	});
	state.staged = true;
	return SinkFinalizeType::READY;
}

void IcebergFileRewrite::StageRewrite(IcebergTransactionData &transaction_data,
                                      const IcebergFileRewriteGlobalState &state,
                                      IcebergSnapshotOperationType operation,
                                      vector<IcebergManifestEntry> &&new_entries,
                                      IcebergManifestDeletes &&replaced_files) const {
	if (operation == IcebergSnapshotOperationType::DELETE) {
		if (!new_entries.empty()) {
			throw InternalException("A file-removal-only rewrite cannot add data files");
		}
		transaction_data.AddDeleteSnapshot(partitioned_manifest_entry_map_t(), std::move(replaced_files));
	} else {
		transaction_data.AddSnapshot(operation, std::move(new_entries), std::move(replaced_files));
	}
}

string IcebergFileRewrite::GetName() const {
	return "ICEBERG_FILE_REWRITE";
}

} // namespace duckdb
