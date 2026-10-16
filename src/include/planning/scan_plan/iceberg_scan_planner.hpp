//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/scan_plan/iceberg_scan_planner.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/function/partition_stats.hpp"

#include "planning/deletes/iceberg_delete_planner.hpp"
#include "planning/pruning/iceberg_table_filter.hpp"
#include "planning/scan_order/iceberg_scan_order.hpp"
#include "planning/scan_plan/iceberg_scan_plan_provider.hpp"
#include "planning/scan_plan/iceberg_scan_plan_state.hpp"
#include "planning/scan_plan/iceberg_scan_task.hpp"

namespace duckdb {

class IcebergTableSchemaVersion;
struct IcebergScanPlanContext;

//! Plans Iceberg data-file scans independently of DuckDB's MultiFileReader API.
//! A planner represents one filtered view; filtered views share the underlying
//! provider and manifest read state but own their pruning, ordering, and cursors.
class IcebergScanPlanner {
public:
	IcebergScanPlanner(ClientContext &context, shared_ptr<IcebergScanInfo> scan_info, const string &path,
	                   const IcebergOptions &options);
	~IcebergScanPlanner();

	unique_ptr<IcebergScanPlanner> CreateView(IcebergTableFilters filters) const;

	void SetTable(IcebergTableSchemaVersion &table);
	optional_ptr<IcebergTableSchemaVersion> GetTable() const;
	void SetScanInfo(shared_ptr<IcebergScanInfo> scan_info);
	void SetOptions(const IcebergOptions &options);
	void SetScanOrder(unique_ptr<RowGroupOrderOptions> options);
	void DisableServerSidePlanning();
	void RequireRowIds();

	const IcebergTableFilters &Filters() const;

	const IcebergTableMetadata &GetMetadata() const;
	const IcebergTableSchema &GetSchema() const;
	ClientContext &GetContext() const;
	const string &GetPath() const;
	const IcebergOptions &GetOptions() const;
	bool HasScanInfo() const;
	bool HasTransactionData() const;
	const IcebergSnapshotScanInfo &GetSnapshot() const;

	optional<IcebergFileScanTask> GetScanTask(idx_t file_id) const;
	//! File enumeration only: does not resolve partition constants or load delete manifests.
	optional<IcebergDataFileDescriptor> GetDataFileDescriptor(idx_t file_id) const;
	idx_t GetTotalFileCount() const;
	unique_ptr<NodeStatistics> GetCardinality() const;
	void GetStatistics(vector<PartitionStatistics> &result) const;
	IcebergPartition GetPartitionForDataFile(const string &file_path) const;

private:
	explicit IcebergScanPlanner(shared_ptr<IcebergScanPlanState> shared_state);

	IcebergScanPlanProvider &GetScanPlanProvider() const DUCKDB_REQUIRES(shared_state->lock);
	IcebergDeletePlanningContext GetDeletePlanningContext() const DUCKDB_REQUIRES(shared_state->lock);

	void InitializeView(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void EnsureScanOrderApplied(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	optional_ptr<const BoundIcebergManifestEntry> GetDataFile(idx_t file_id,
	                                                          annotated_lock_guard<annotated_mutex> &guard) const
	    DUCKDB_REQUIRES(shared_state->lock);
	IcebergDataFileDescriptor CreateDataFileDescriptor(const BoundIcebergManifestEntry &entry) const
	    DUCKDB_REQUIRES(shared_state->lock);
	bool TryGetNextBatch(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void FinishScanTasks(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void StartDataManifestScan(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	vector<IcebergDeleteFile> ResolveApplicableDeleteFiles(const BoundIcebergManifestEntry &data_manifest_entry) const;

private:
	shared_ptr<IcebergScanPlanState> shared_state;
	ClientContext &context;
	FileSystem &fs;
	IcebergTableFilters table_filters;

	mutable bool view_initialized DUCKDB_GUARDED_BY(shared_state->lock) = false;
	mutable vector<BoundIcebergManifestListEntry> delete_manifests DUCKDB_GUARDED_BY(shared_state->lock);
	mutable vector<bool> delete_manifest_matches DUCKDB_GUARDED_BY(shared_state->lock);
	mutable atomic<bool> has_matching_delete_manifests {true};
	mutable IcebergDataViewCursor data_view_cursor DUCKDB_GUARDED_BY(shared_state->lock);
	mutable vector<BoundIcebergManifestEntry> data_manifest_entries DUCKDB_GUARDED_BY(shared_state->lock);
	mutable vector<BoundIcebergManifestListEntry> data_manifests DUCKDB_GUARDED_BY(shared_state->lock);
	mutable vector<bool> data_manifest_matches DUCKDB_GUARDED_BY(shared_state->lock);
	mutable IcebergScanOrder scan_order DUCKDB_GUARDED_BY(shared_state->lock);
};

} // namespace duckdb
