#pragma once

#include "core/deletes/iceberg_delete_data.hpp"
#include "core/deletes/iceberg_equality_delete.hpp"
#include "core/deletes/iceberg_delete_file.hpp"

#include <condition_variable>

namespace duckdb {

using position_delete_map_t = unordered_map<string, shared_ptr<IcebergDeleteData>>;

struct IcebergDeletePlan {
	//! Equality-delete values are materialized separately from positional deletes, which become a DeleteFilter.
	vector<reference<const IcebergEqualityDeleteFile>> equality_deletes;
	unique_ptr<DeleteFilter> positional_deletes;
};

//! Only execution dependencies: no manifest discovery, filtering, or scan-plan provider.
struct IcebergDeleteExecutionContext {
	ClientContext &context;
	FileSystem &fs;
	const string &table_path;
	const IcebergOptions &options;
	const IcebergTableMetadata &metadata;
};

//! Contents are immutable after publication through complete under lock.
//! Execution state for one delete file. This deliberately lives outside scan
//! planning: it caches the result of reading the selected delete descriptor.
struct IcebergDeleteFileLoadState {
	mutex lock;
	std::condition_variable cv;
	bool complete = false;
	ErrorData error;
	shared_ptr<IcebergEqualityDeleteFile> equality_delete;
	position_delete_map_t positional_deletes;
};

//! A selected descriptor and the load state exclusively populated by its builder.
struct IcebergDeleteScanEntry {
	IcebergDeleteFile file;
	shared_ptr<IcebergDeleteFileLoadState> load;
};

//! Intermediate to store the created delete file before adding it to the LoadState
struct IcebergEqualityDeleteScanResult {
	//! The LoadState to store the result into, after grabbing the lock
	shared_ptr<IcebergDeleteFileLoadState> load;
	//! The resulting equality delete data
	shared_ptr<IcebergEqualityDeleteFile> delete_file;
};

//! Equality-delete results to publish after the batch completes.
//! Positional contents are built in each file's exclusively owned load state.
struct IcebergDeleteScanResult {
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
	IcebergDeletePlan ProcessDeletes(const IcebergDeleteExecutionContext &context, const string &data_file_path,
	                                 const vector<IcebergDeleteFile> &descriptors);
	shared_ptr<IcebergDeleteData> GetExistingPositionalDeleteData(const string &file_path) const;

private:
	mutable mutex lock;
	//! Assembled task results retained for DELETE/UPDATE, never used to filter another task.
	position_delete_map_t positional_delete_data;
	//! File identities select buckets; complete descriptors retain scan semantics.
	unordered_map<IcebergFileIdentity, vector<pair<IcebergDeleteFile, shared_ptr<IcebergDeleteFileLoadState>>>,
	              IcebergFileIdentityHash>
	    descriptor_loads;
};

} // namespace duckdb
