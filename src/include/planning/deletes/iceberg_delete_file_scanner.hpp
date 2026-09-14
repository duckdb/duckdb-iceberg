#pragma once

#include "planning/deletes/iceberg_delete_planner.hpp"

#include <condition_variable>

namespace duckdb {

class IcebergScanPlanner;
struct IcebergDeleteFileReference;
struct IcebergScanTask;

//! Only execution dependencies: no manifest discovery, filtering, or scan-plan provider.
struct IcebergDeleteExecutionContext {
	ClientContext &context;
	FileSystem &fs;
	const string &table_path;
	const IcebergOptions &options;
	const IcebergTableMetadata &metadata;
};

//! Execution state for one delete file. This deliberately lives outside scan
//! planning: it caches the result of reading the selected delete descriptor.
struct IcebergDeleteFileLoadState {
	mutex lock;
	std::condition_variable cv;
	bool complete = false;
	ErrorData error;
	shared_ptr<IcebergEqualityDeleteFile> equality_delete;
};

//! Input to ScanFiles, so it can run without holding the (delete_)lock
struct IcebergDeleteScanEntry {
	IcebergDeleteScanEntry(idx_t manifest_idx_p, idx_t entry_idx_p, const IcebergManifestListEntry &manifest_p,
	                       shared_ptr<IcebergDeleteFileLoadState> load_p)
	    : manifest_idx(manifest_idx_p), entry_idx(entry_idx_p), manifest(manifest_p), load(std::move(load_p)) {
	}

	const IcebergManifestEntry &GetEntry() const;
	BoundIcebergManifestEntry BindEntry() const;

	idx_t manifest_idx;
	idx_t entry_idx;
	const IcebergManifestListEntry &manifest;
	//! The shared LoadState of the delete file to populate
	shared_ptr<IcebergDeleteFileLoadState> load;
};

//! Intermediate to store the created delete file before adding it to the LoadState
struct IcebergEqualityDeleteScanResult {
	//! The LoadState to store the result into, after grabbing the lock
	shared_ptr<IcebergDeleteFileLoadState> load;
	//! The resulting equality delete data
	shared_ptr<IcebergEqualityDeleteFile> delete_file;
};

//! Grouped result of all delete files scanned for a data file
struct IcebergDeleteScanResult {
	position_delete_map_t positional_delete_data;
	vector<IcebergEqualityDeleteScanResult> equality_delete_data;
};

struct IcebergDeleteFileScanner {
	static IcebergDeleteScanResult ScanFiles(const IcebergDeleteExecutionContext &context,
	                                         const vector<IcebergDeleteScanEntry> &entries);
};

//! Materialized delete contents and synchronization for executing planned tasks.
//! The scan planner only selects descriptors; this state reads and caches them.
class IcebergDeleteExecutionState {
public:
	IcebergDeletePlan ProcessDeletes(const IcebergScanPlanner &planner, const IcebergScanTask &task);
	shared_ptr<IcebergDeleteData> GetExistingPositionalDeleteData(const string &file_path) const;

private:
	shared_ptr<IcebergDeleteFileLoadState> &GetDeleteFileLoad(const IcebergScanPlanner &planner,
	                                                          IcebergDeleteFileReference delete_file);

private:
	mutable mutex lock;
	vector<unordered_map<idx_t, shared_ptr<IcebergDeleteFileLoadState>>> delete_file_loads;
	position_delete_map_t positional_delete_data;
};

} // namespace duckdb
