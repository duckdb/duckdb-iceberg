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

class IcebergTableEntry;
class IcebergScanPlanProvider;
struct IcebergScanPlanContext;
struct IcebergMultiFileList;
struct IcebergMultiFileReader;
struct IcebergDeleteFileReference;

struct IcebergMultiFileList : public MultiFileList {
public:
	//! A data file whose every row is provably covered by the (fully pushed-down) scan filter, recorded
	//! during a metadata-only DELETE so the file can be dropped without a row-level scan.
	struct CoveredDataFile {
		//! Locates the manifest entry, so the delete files applying to it can be resolved once the scan
		//! is done.
		idx_t manifest_file_idx;
		idx_t manifest_entry_idx;
		//! The data file's path as recorded in the manifest, which is what InvalidateFile matches on
		string file_path;
		//! Rows physically written to the file, before any delete file is applied
		int64_t record_count;
	};

	//! A covered data file resolved against the delete files that apply to it: everything the delete
	//! needs to drop it, with no further metadata lookups.
	struct DroppedDataFile {
		//! The data file to invalidate
		string file_path;
		//! Rows the drop actually removes: the record count minus the rows already removed by the delete
		//! files below. Deletes whose rows cannot be attributed to this file alone - equality deletes, and
		//! positional deletes that do not name it - are not subtracted, so this can over-report for them.
		int64_t live_record_count;
		//! Delete files that apply to this data file only, and are therefore invalidated along with it
		vector<string> superseded_delete_files;
	};

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
	void SetTable(IcebergTableEntry &table);
	shared_ptr<IcebergDeleteData> GetExistingPositionalDeleteData(const string &file_path) const;
	IcebergPartition GetPartitionForDataFile(const string &file_path) const;
	void SetScanOrder(unique_ptr<RowGroupOrderOptions> options);
	optional_ptr<IcebergTableEntry> GetTable() const;
	void DisableServerSidePlanning();

	//! Enable plan-time metadata-only DELETE: the scan driving the delete records fully-covered data files
	//! (those every row of which matches the delete predicate) instead of emitting their rows.
	void EnableMetadataOnlyDelete();
	//! Whether this scan may drop fully-covered data files instead of emitting their rows
	bool IsMetadataOnlyDeleteEnabled() const;
	//! The files the scan proved fully covered, resolved against the delete files applying to them. Reads
	//! delete-manifest metadata only - the delete files themselves are never opened.
	vector<DroppedDataFile> ResolveDroppedDataFiles() const;

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
	//! Whether an alter staged earlier in this transaction dropped the file at the manifest level
	bool IsInvalidatedInTransaction(const string &file_path) const;
	//! Cached per partition spec, since the answer does not depend on the data file. Keeps the per-file
	//! coverage proof off the scan path entirely for deletes no partition field could ever cover.
	bool PartitionSpecHasFieldForEveryFilterColumn(int32_t partition_spec_id,
	                                               annotated_lock_guard<annotated_mutex> &guard) const
	    DUCKDB_REQUIRES(shared_state->lock);
	const IcebergSnapshotScanInfo &GetSnapshot() const;

	unique_ptr<IcebergMultiFileList> PushdownInternal(ClientContext &context, TableFilterSet &new_filters,
	                                                  const vector<ColumnIndex> &column_indexes,
	                                                  bool filters_fully_pushed = false) const;
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
	//! True when the current filter set was pushed down in its entirety (every filter PUSHED_DOWN_FULLY),
	//! so there is no residual filter above the scan. Required before any file may be dropped by a delete.
	bool filters_fully_pushed = false;
	//! True only for the scan that drives a metadata-only DELETE. Gates the skip+register branch in GetDataFile
	//! so ordinary SELECTs never drop covered files.
	bool metadata_only_delete_enabled = false;
	//! Data files whose every row is covered by the fully pushed-down delete predicate, recorded during the scan.
	mutable vector<CoveredDataFile> fully_covered_data_files DUCKDB_GUARDED_BY(shared_state->lock);
	//! partition_spec_id -> whether a file under it could be fully covered by the current filters
	mutable unordered_map<int32_t, bool> spec_has_fields_for_filters DUCKDB_GUARDED_BY(shared_state->lock);

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
