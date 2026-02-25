//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_multi_file_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "storage/iceberg_metadata_info.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "manifest_reader.hpp"

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/list.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "deletes/equality_delete.hpp"
#include "deletes/positional_delete.hpp"
#include "deletes/iceberg_delete_data.hpp"
#include "avro_scan.hpp"
#include "duckdb/parallel/task_executor.hpp"

namespace duckdb {

struct IcebergManifestReadingState {
public:
	IcebergManifestReadingState(ClientContext &context, unique_ptr<AvroScan> scan, mutex &lock,
	                            vector<IcebergManifestEntry> &entries)
	    : context(context), executor(context), scan(std::move(scan)), lock(lock), entries(entries),
	      in_progress_tasks(0) {
	}

public:
	ClientContext &context;
	TaskExecutor executor;
	unique_ptr<AvroScan> scan;
	mutex &lock;
	vector<IcebergManifestEntry> &entries;
	atomic<idx_t> in_progress_tasks;
};

enum class IcebergDataFileType : uint8_t { DATA, DELETE };

struct IcebergMultiFileList : public MultiFileList {
public:
	IcebergMultiFileList(ClientContext &context, shared_ptr<IcebergScanInfo> scan_info, const string &path,
	                     const IcebergOptions &options);
	virtual ~IcebergMultiFileList() override;

public:
	static string ToDuckDBPath(const string &raw_path);
	string GetPath() const;
	const IcebergTableMetadata &GetMetadata() const;
	bool HasTransactionData() const;
	const IcebergTransactionData &GetTransactionData() const;
	optional_ptr<const IcebergSnapshot> GetSnapshot() const;
	const IcebergTableSchema &GetSchema() const;
	bool FinishedScanningDeletes() const;

	void Bind(vector<LogicalType> &return_types, vector<string> &names);
	unique_ptr<IcebergMultiFileList> PushdownInternal(ClientContext &context, TableFilterSet &new_filters) const;
	void ScanPositionalDeleteFile(const IcebergManifestEntry &manifest_entry, DataChunk &result) const;
	void ScanEqualityDeleteFile(const IcebergManifestEntry &manifest_entry, DataChunk &result,
	                            vector<MultiFileColumnDefinition> &columns,
	                            const vector<MultiFileColumnDefinition> &global_columns,
	                            const vector<ColumnIndex> &column_indexes) const;
	void ScanDeleteFile(const IcebergManifestEntry &entry, const vector<MultiFileColumnDefinition> &global_columns,
	                    const vector<ColumnIndex> &column_indexes) const;
	void ScanPuffinFile(const IcebergManifestEntry &entry) const;
	unique_ptr<DeleteFilter> GetPositionalDeletesForFile(const string &file_path) const;
	void ProcessDeletes(const vector<MultiFileColumnDefinition> &global_columns,
	                    const vector<ColumnIndex> &column_indexes) const;
	void GetStatistics(vector<PartitionStatistics> &result) const;

public:
	//! MultiFileList API
	unique_ptr<MultiFileList> DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                const vector<string> &names, const vector<LogicalType> &types,
	                                                const vector<column_t> &column_ids,
	                                                TableFilterSet &filters) const override;
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) const override;
	vector<OpenFileInfo> GetAllFiles() const override;
	FileExpandResult GetExpandResult() const override;
	idx_t GetTotalFileCount() const override;
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context) const override;

protected:
	//! Get the i-th expanded file
	OpenFileInfo GetFile(idx_t i) const override;
	OpenFileInfo GetFileInternal(idx_t i, lock_guard<mutex> &guard) const;

protected:
	bool ManifestMatchesFilter(const IcebergManifestFile &manifest) const;
	bool FileMatchesFilter(const IcebergManifestEntry &file, IcebergDataFileType file_type) const;
	// TODO: How to guarantee we only call this after the filter pushdown?
	void InitializeFiles(lock_guard<mutex> &guard) const;

	//! NOTE: this requires the lock because it modifies the 'data_files' vector, potentially invalidating references
	optional_ptr<const IcebergManifestEntry> GetDataFile(idx_t file_id, lock_guard<mutex> &guard) const;

	optional_ptr<const TableFilter> GetFilterForColumnIndex(const TableFilterSet &filter_set,
	                                                        const ColumnIndex &column_index) const;

private:
	bool PopulateEntryBuffer(lock_guard<mutex> &guard) const;
	void FinishScanTasks(lock_guard<mutex> &guard) const;

public:
	ClientContext &context;
	FileSystem &fs;
	shared_ptr<IcebergScanInfo> scan_info;
	string path;
	IcebergTableEntry *table;

	mutable mutex lock;
	//! ComplexFilterPushdown results
	bool have_bound = false;
	vector<string> names;
	vector<LogicalType> types;
	TableFilterSet table_filters;

	mutable mutex entry_lock;
	mutable vector<IcebergManifestEntry> manifest_entries;
	mutable vector<IcebergManifestEntry> delete_manifest_entries;
	//! For each file that has a delete file, the state for processing that/those delete file(s)
	mutable case_insensitive_map_t<shared_ptr<IcebergDeleteData>> positional_delete_data;
	//! All equality deletes with sequence numbers higher than that of the data_file apply to that data_file
	mutable map<sequence_number_t, unique_ptr<IcebergEqualityDeleteData>> equality_delete_data;

	//! State used for lazy-loading the data files
	mutable unique_ptr<manifest_file::ManifestReader> data_manifest_reader;
	mutable idx_t manifest_entry_idx = 0;
	//! The data files of the manifest file that we last scanned
	mutable vector<IcebergManifestEntry> current_manifest_entries;
	mutable vector<IcebergManifestFile> data_manifests;
	mutable vector<reference<IcebergManifest>> transaction_data_manifests;
	mutable idx_t transaction_data_idx = 0;
	mutable unique_ptr<IcebergManifestReadingState> manifest_read_state;
	mutable atomic<bool> finished;
	mutable atomic<bool> has_buffered_entries;

	//! State used for pre-processing delete files
	mutable unique_ptr<AvroScan> delete_manifest_scan;
	mutable unique_ptr<manifest_file::ManifestReader> delete_manifest_reader;
	mutable vector<IcebergManifestFile> delete_manifests;
	mutable vector<reference<IcebergManifest>> transaction_delete_manifests;
	mutable idx_t transaction_delete_idx = 0;

	//! FIXME: this is only used in 'FinalizeBind',
	//! shouldn't this be used to protect all the variable accesses that are accessed there while the lock is held?
	mutable mutex delete_lock;

	mutable bool initialized = false;
	const IcebergOptions &options;
};

} // namespace duckdb
