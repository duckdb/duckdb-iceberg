//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/iceberg_multi_file_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/list.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

#include "common/iceberg_utils.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"
#include "planning/deletes/iceberg_delete_planner.hpp"
#include "planning/pruning/iceberg_table_filter.hpp"
#include "planning/scan_order/iceberg_scan_order.hpp"
#include "planning/scan_plan/iceberg_scan_plan_state.hpp"

namespace duckdb {

class IcebergTableSchemaVersion;
class IcebergScanPlanProvider;
struct IcebergScanPlanContext;
struct IcebergMultiFileList;
struct IcebergMultiFileReader;
struct IcebergDeleteFileReference;

struct IcebergMultiFileList : public MultiFileList {
public:
	IcebergMultiFileList(ClientContext &context, shared_ptr<IcebergScanInfo> scan_info, const string &path,
	                     const IcebergOptions &options);
	virtual ~IcebergMultiFileList() override;

public:
	//! MultiFileList API
	unique_ptr<MultiFileList> DynamicFilterPushdown(MultiFileDynamicPushdownInfo &pushdown_info) const override;
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) const override;
	vector<OpenFileInfo> GetAllFiles() const override;
	FileExpandResult GetExpandResult() const override;
	idx_t GetTotalFileCount() const override;
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context) const override;
	OpenFileInfo GetFile(idx_t i) const override;

public:
	void SetTable(IcebergTableSchemaVersion &table);
	shared_ptr<IcebergDeleteData> GetExistingPositionalDeleteData(const string &file_path) const;
	IcebergPartition GetPartitionForDataFile(const string &file_path) const;
	void SetScanOrder(unique_ptr<RowGroupOrderOptions> options);
	optional_ptr<IcebergTableSchemaVersion> GetTable() const;
	void DisableServerSidePlanning();

	//! Narrow integration surface used by IcebergMultiFileReader.
	void SetOptions(const IcebergOptions &options);
	void Bind(vector<LogicalType> &return_types, vector<Identifier> &names);
	void GetStatistics(vector<PartitionStatistics> &result) const;
	const IcebergTableMetadata &GetMetadata() const;
	const IcebergTableSchema &GetSchema() const;
	BoundIcebergManifestEntry GetManifestEntry(idx_t file_id) const;
	IcebergManifestFile GetManifestFileForDataFile(idx_t file_id) const;
	IcebergDeletePlan ProcessDeletes(const BoundIcebergManifestEntry &data_manifest_entry) const;

private:
	const string &GetPath() const;
	const IcebergTransactionData &GetTransactionData() const;
	const IcebergSnapshotScanInfo &GetSnapshot() const;

	unique_ptr<IcebergMultiFileList> PushdownInternal(ClientContext &context, TableFilterSet &new_filters,
	                                                  const vector<ColumnIndex> &column_indexes) const;
	IcebergMultiFileList(shared_ptr<IcebergScanPlanState> shared_state);

	void InitializeView(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);

	//! The delete-manifest entries that apply to a data file, after manifest pruning and the per-entry
	//! filter and data-file checks. Must be called without holding the shared lock - reading the delete
	//! manifests takes it.
	vector<IcebergDeleteFileReference>
	ResolveApplicableDeleteFiles(const BoundIcebergManifestEntry &data_manifest_entry) const;

	bool HasTransactionData() const;
	//! Reorder (and prune, when a LIMIT is present) the materialized data files by the
	//! ORDER BY column's per-file min/max bounds, mirroring the native RowGroupReorderer.
	void EnsureScanOrderApplied(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	OpenFileInfo GetFileInternal(idx_t i, annotated_lock_guard<annotated_mutex> &guard) const
	    DUCKDB_REQUIRES(shared_state->lock);
	const IcebergManifestFile &GetManifestFileForEntry(const BoundIcebergManifestEntry &entry,
	                                                   IcebergManifestContentType type) const
	    DUCKDB_REQUIRES(shared_state->lock);

	//! Whether a delete file's manifest entry can apply to any file selected by the current scan filter.
	//! Delete files are pruned on partition only: one whose partition is excluded by the filter cannot
	//! delete a row from any surviving data file, so it does not need to be read.
	//! NOTE: this requires the lock because it modifies the 'data_files' vector, potentially invalidating references
	optional_ptr<const BoundIcebergManifestEntry> GetDataFile(idx_t file_id,
	                                                          annotated_lock_guard<annotated_mutex> &guard) const
	    DUCKDB_REQUIRES(shared_state->lock);

	bool TryGetNextBatch(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void FinishScanTasks(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void LoadManifestList(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	void InitializeScanPlanProvider() const DUCKDB_REQUIRES(shared_state->lock);
	void StartDataManifestScan(annotated_lock_guard<annotated_mutex> &guard) const DUCKDB_REQUIRES(shared_state->lock);
	IcebergScanPlanProvider &GetScanPlanProvider() const DUCKDB_REQUIRES(shared_state->lock);
	IcebergScanPlanContext GetScanPlanContext() const DUCKDB_REQUIRES(shared_state->lock);
	IcebergDeletePlanningContext GetDeletePlanningContext() const DUCKDB_REQUIRES(shared_state->lock);

private:
	shared_ptr<IcebergScanPlanState> shared_state;
	ClientContext &context;
	FileSystem &fs;
	const IcebergOptions &options;
	//! ComplexFilterPushdown results
	bool have_bound = false;
	vector<string> names;
	vector<LogicalType> types;
	IcebergTableFilters table_filters;

	//! The provider is per-view. The server-side implementation owns its filter-derived plan, while the client-side
	//! implementation delegates to the shared manifest state above.
	mutable unique_ptr<IcebergScanPlanProvider> scan_plan_provider DUCKDB_GUARDED_BY(shared_state->lock);

	//! Combination of committed + transaction delete manifests
	mutable vector<BoundIcebergManifestListEntry> delete_manifests DUCKDB_GUARDED_BY(shared_state->lock);
	mutable vector<bool> delete_manifest_matches DUCKDB_GUARDED_BY(shared_state->lock);
	//! Conservative until InitializeView determines whether this filtered view has any matching delete manifests.
	mutable atomic<bool> has_matching_delete_manifests {true};

	mutable IcebergDataViewCursor data_view_cursor DUCKDB_GUARDED_BY(shared_state->lock);
	//! References to items inside the 'manifest_entries' of the list entries in the 'data_manifests'
	mutable vector<BoundIcebergManifestEntry> data_manifest_entries DUCKDB_GUARDED_BY(shared_state->lock);
	//! Combination of committed + transaction data manifests
	mutable vector<BoundIcebergManifestListEntry> data_manifests DUCKDB_GUARDED_BY(shared_state->lock);
	mutable vector<bool> data_manifest_matches DUCKDB_GUARDED_BY(shared_state->lock);

	//! Set by the table function's set_scan_order callback when an ORDER BY ... LIMIT can drive scan order.
	mutable IcebergScanOrder scan_order DUCKDB_GUARDED_BY(shared_state->lock);
};

} // namespace duckdb
