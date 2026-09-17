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

namespace duckdb {

class IcebergTableSchemaVersion;
struct IcebergScanPlanContext;

//! A query-lifetime view of all metadata needed to initialize one data-file scan.
//! The manifest entries referenced by this object are owned by the planner.
struct IcebergScanTask {
	BoundIcebergManifestEntry manifest_entry;
	string file_path;
	vector<IcebergDeleteFileReference> delete_files;
};

//! Plans Iceberg data-file scans independently of DuckDB's MultiFileReader API.
//! A planner represents one filtered view; filtered views share the underlying
//! manifest read state but own their provider, pruning, ordering, and cursors.
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

	const IcebergTableFilters &Filters() const;

	const IcebergTableMetadata &GetMetadata() const;
	const IcebergTableSchema &GetSchema() const;
	ClientContext &GetContext() const;
	const string &GetPath() const;
	const IcebergOptions &GetOptions() const;
	bool HasScanInfo() const;

	optional<IcebergScanTask> GetScanTask(idx_t file_id) const;
	optional<IcebergScanTask> GetDataFileTask(idx_t file_id) const;
	optional_ptr<const BoundIcebergManifestEntry> GetDataFile(idx_t file_id) const;
	idx_t GetTotalFileCount() const;
	unique_ptr<NodeStatistics> GetCardinality() const;
	void GetStatistics(vector<PartitionStatistics> &result) const;
	IcebergPartition GetPartitionForDataFile(const string &file_path) const;
	const IcebergManifestListEntry &GetDeleteManifest(IcebergDeleteFileReference delete_file) const;
	unique_ptr<IcebergDeletePlanningContext> CreateDeletePlanningContext() const;
	void WithManifestFile(const BoundIcebergManifestEntry &entry, IcebergManifestContentType type,
	                      const std::function<void(const IcebergManifestFile &manifest_file)> &callback) const
	    DUCKDB_EXCLUDES(shared_state->lock);

private:
	explicit IcebergScanPlanner(shared_ptr<IcebergScanPlanState> shared_state);

	bool HasTransactionData() const;
	const IcebergTransactionData &GetTransactionData() const;
	const IcebergSnapshotScanInfo &GetSnapshot() const;
	IcebergScanPlanProvider &GetScanPlanProvider() const DUCKDB_REQUIRES(shared_state->lock);
	IcebergScanPlanContext GetScanPlanContext() const DUCKDB_REQUIRES(shared_state->lock);
	IcebergDeletePlanningContext GetDeletePlanningContext() const DUCKDB_REQUIRES(shared_state->lock);

	void InitializeView(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void EnsureScanOrderApplied(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	optional_ptr<const BoundIcebergManifestEntry> GetDataFile(idx_t file_id,
	                                                          annotated_lock_guard<annotated_mutex> &guard) const
	    DUCKDB_REQUIRES(shared_state->lock);
	const IcebergManifestFile &GetManifestFileForEntry(const BoundIcebergManifestEntry &entry,
	                                                   IcebergManifestContentType type) const
	    DUCKDB_REQUIRES(shared_state->lock);
	bool TryGetNextBatch(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void FinishScanTasks(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void LoadManifestList(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void InitializeScanPlanProvider() const DUCKDB_REQUIRES(shared_state->lock);
	void StartDataManifestScan(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	vector<IcebergDeleteFileReference>
	ResolveApplicableDeleteFiles(const BoundIcebergManifestEntry &data_manifest_entry) const;

private:
	shared_ptr<IcebergScanPlanState> shared_state;
	ClientContext &context;
	FileSystem &fs;
	const IcebergOptions &options;
	IcebergTableFilters table_filters;

	mutable unique_ptr<IcebergScanPlanProvider> scan_plan_provider DUCKDB_GUARDED_BY(shared_state->lock);
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
