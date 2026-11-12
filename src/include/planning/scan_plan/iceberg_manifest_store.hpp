#pragma once

#include "planning/scan_plan/iceberg_scan_plan_context.hpp"
#include "planning/iceberg_manifest_read_state.hpp"
#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"

namespace duckdb {

struct IcebergManifestScanningState;
struct IcebergDeleteManifestLoadState;

//! Client-side manifest storage and I/O shared by filtered scan views.
//! The caller's planning lock protects publication; delete I/O runs outside it.
//! Owns background data-read tasks and drains them before releasing their storage.
class IcebergManifestStore {
public:
	IcebergManifestStore(annotated_mutex &lock, IcebergScanPlanContext context);
	~IcebergManifestStore();

	void LoadManifestList() DUCKDB_REQUIRES(lock);
	void StartDataManifestScan(const vector<bool> &matching_manifests, idx_t filter_count) DUCKDB_REQUIRES(lock);
	void ReadDeleteManifests(const vector<idx_t> &manifest_indexes, idx_t filter_count);
	vector<IcebergDeleteFileReference> GetDeleteFiles(const vector<idx_t> &manifest_indexes) DUCKDB_REQUIRES(lock);
	bool TryGetNextBatch(IcebergDataViewCursor &cursor) DUCKDB_REQUIRES(lock);
	void FinishScanTasks() DUCKDB_REQUIRES(lock);
	const vector<IcebergManifestListEntry> &DataManifests() DUCKDB_REQUIRES(lock);
	const vector<IcebergManifestListEntry> &DeleteManifests() DUCKDB_REQUIRES(lock);
	vector<reference<const IcebergManifestListEntry>> TransactionDataManifests() const DUCKDB_REQUIRES(lock);
	vector<reference<const IcebergManifestListEntry>> TransactionDeleteManifests() const DUCKDB_REQUIRES(lock);

	// Shared with the planner so manifest publication and view binding use the same lock.
	annotated_mutex &lock;

private:
	IcebergScanPlanContext context;
	annotated_mutex delete_manifest_lock DUCKDB_ACQUIRED_AFTER(lock);
	ManifestEntryReadState read_state;

	bool manifest_list_loaded DUCKDB_GUARDED_BY(lock) = false;
	bool data_manifest_scan_started DUCKDB_GUARDED_BY(lock) = false;

	vector<IcebergManifestListEntry> committed_delete_manifests DUCKDB_GUARDED_BY(lock);
	//! Owns the scan views of pending content; these never refer to Avro files.
	vector<IcebergManifestListEntry> transaction_delete_manifests DUCKDB_GUARDED_BY(lock);
	vector<shared_ptr<IcebergDeleteManifestLoadState>> delete_manifest_loads DUCKDB_GUARDED_BY(delete_manifest_lock);

	vector<IcebergManifestListEntry> committed_data_manifests DUCKDB_GUARDED_BY(lock);
	//! Keep track of which manifests we had to eagerly load, so we can emit batches for them once the data scan is
	//! started
	vector<bool> eagerly_loaded_data_manifests DUCKDB_GUARDED_BY(lock);
	vector<IcebergManifestListEntry> transaction_data_manifests DUCKDB_GUARDED_BY(lock);
	unique_ptr<IcebergManifestScanningState> data_manifest_read_state DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
