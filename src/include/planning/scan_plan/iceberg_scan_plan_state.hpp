#pragma once

#include "core/deletes/iceberg_delete_data.hpp"
#include "core/deletes/iceberg_equality_delete.hpp"
#include "core/deletes/iceberg_positional_delete.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "planning/iceberg_manifest_read_state.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"
#include "planning/snapshot/iceberg_scan_info.hpp"

#include <condition_variable>

namespace duckdb {

class IcebergTableSchemaVersion;
struct IcebergDeleteManifestLoadState;

struct IcebergDeleteFileLoadState {
	mutex lock;
	std::condition_variable cv;
	bool complete = false;
	ErrorData error;
	shared_ptr<IcebergEqualityDeleteFile> equality_delete;
};

struct IcebergManifestScanningState {
	IcebergManifestScanningState(ClientContext &context, unique_ptr<AvroScan> scan,
	                             vector<IcebergManifestListEntry> &list_entries)
	    : context(context), executor(context), scan(std::move(scan)), list_entries(list_entries), in_progress_tasks(0) {
	}

	ClientContext &context;
	TaskExecutor executor;
	unique_ptr<AvroScan> scan;
	vector<IcebergManifestListEntry> &list_entries;
	atomic<idx_t> in_progress_tasks;
};

struct IcebergDataViewCursor {
	idx_t next_batch_idx = 0;
	bool has_current_batch = false;
	ManifestReadBatch current_batch;
	idx_t current_batch_offset = 0;
};

//! State shared by filtered views of one Iceberg scan. Providers own the algorithms
//! that mutate this state; the multi-file list only coordinates lock transitions.
struct IcebergScanPlanState {
	IcebergScanPlanState(ClientContext &context, shared_ptr<IcebergScanInfo> scan_info, string path,
	                     const IcebergOptions &options);
	~IcebergScanPlanState();

	ClientContext &context;
	FileSystem &fs;
	shared_ptr<IcebergScanInfo> scan_info;
	string path;
	optional_ptr<IcebergTableSchemaVersion> table;
	IcebergOptions options;

	mutable annotated_mutex lock;
	mutable annotated_mutex delete_lock DUCKDB_ACQUIRED_AFTER(lock);
	mutable ManifestEntryReadState read_state;

	mutable bool server_side_planning_enabled DUCKDB_GUARDED_BY(lock) = true;
	mutable bool manifest_list_loaded DUCKDB_GUARDED_BY(lock) = false;
	mutable bool data_manifest_scan_started DUCKDB_GUARDED_BY(lock) = false;

	mutable vector<IcebergManifestListEntry> committed_delete_manifests DUCKDB_GUARDED_BY(lock);
	mutable vector<reference<const IcebergManifestListEntry>> transaction_delete_manifests DUCKDB_GUARDED_BY(lock);
	mutable vector<shared_ptr<IcebergDeleteManifestLoadState>> delete_manifest_loads DUCKDB_GUARDED_BY(delete_lock);
	mutable vector<unordered_map<idx_t, shared_ptr<IcebergDeleteFileLoadState>>>
	    delete_file_loads DUCKDB_GUARDED_BY(delete_lock);

	mutable vector<IcebergManifestListEntry> committed_data_manifests DUCKDB_GUARDED_BY(lock);
	//! Keep track of which manifests we had to eagerly load, so we can emit batches for them once the data scan is
	//! started
	mutable vector<bool> eagerly_loaded_data_manifests DUCKDB_GUARDED_BY(lock);
	mutable vector<reference<const IcebergManifestListEntry>> transaction_data_manifests DUCKDB_GUARDED_BY(lock);
	mutable unique_ptr<IcebergManifestScanningState> data_manifest_read_state DUCKDB_GUARDED_BY(lock);

	//! Declared after manifest owners so references in parsed positional-delete data are destroyed first.
	mutable unordered_map<string, shared_ptr<IcebergDeleteData>> positional_delete_data DUCKDB_GUARDED_BY(delete_lock);

	mutable unordered_map<string, IcebergPartition> data_file_partitions DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
