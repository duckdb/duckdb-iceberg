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
#include "duckdb/parallel/task_executor.hpp"

#include "common/iceberg_utils.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "core/deletes/iceberg_equality_delete.hpp"
#include "core/deletes/iceberg_positional_delete.hpp"
#include "core/deletes/iceberg_delete_data.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "planning/snapshot/iceberg_scan_info.hpp"
#include "planning/iceberg_manifest_read_state.hpp"
#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"

namespace duckdb {

struct IcebergTableFilters {
	using filter_set_t = unordered_map<idx_t, unique_ptr<ExpressionFilter>>;
	using iterator = filter_set_t::iterator;
	using const_iterator = filter_set_t::const_iterator;

public:
	bool HasFilters() const {
		return !table_filters.empty();
	}
	void PushFilter(column_t column_idx, unique_ptr<ExpressionFilter> table_filter) {
		table_filters[column_idx] = std::move(table_filter);
	}
	optional_ptr<const ExpressionFilter> TryGetFilterByColumnIndex(column_t column_idx) const {
		auto entry = table_filters.find(column_idx);
		if (entry == table_filters.end()) {
			return nullptr;
		}
		return entry->second.get();
	}

	iterator begin() { // NOLINT: match stl API
		return table_filters.begin();
	}
	iterator end() { // NOLINT: match stl API
		return table_filters.end();
	}
	const_iterator begin() const { // NOLINT: match stl API
		return table_filters.begin();
	}
	const_iterator end() const { // NOLINT: match stl API
		return table_filters.end();
	}

private:
	filter_set_t table_filters;
};

class IcebergTableEntry;
struct IcebergMultiFileList;

struct IcebergManifestScanningState {
public:
	IcebergManifestScanningState(ClientContext &context, unique_ptr<AvroScan> scan,
	                             vector<IcebergManifestListEntry> &list_entries)
	    : context(context), executor(context), scan(std::move(scan)), list_entries(list_entries), in_progress_tasks(0) {
	}

public:
	ClientContext &context;
	TaskExecutor executor;
	unique_ptr<AvroScan> scan;
	vector<IcebergManifestListEntry> &list_entries;
	atomic<idx_t> in_progress_tasks;
};

struct IcebergMultiFileListSharedState {
public:
	IcebergMultiFileListSharedState(ClientContext &context, shared_ptr<IcebergScanInfo> scan_info, string path,
	                                const IcebergOptions &options);
	~IcebergMultiFileListSharedState();

private:
	friend struct IcebergMultiFileList;

	ClientContext &context;
	FileSystem &fs;
	shared_ptr<IcebergScanInfo> scan_info;
	string path;
	IcebergTableEntry *table = nullptr;
	IcebergOptions options;

	mutable mutex lock;
	mutable mutex delete_lock;
	mutable ManifestEntryReadState read_state;

	mutable bool initialized = false;

	//! Scanned delete manifests and their owners.
	mutable vector<IcebergManifestListEntry> committed_delete_manifests;
	mutable vector<reference<const IcebergManifestListEntry>> transaction_delete_manifests;
	mutable unique_ptr<AvroScan> delete_manifest_scan;
	mutable unique_ptr<manifest_file::ManifestReader> delete_manifest_reader;
	mutable bool delete_entries_enumerated = false;
	mutable idx_t next_delete_entry_to_process = 0;
	mutable vector<BoundIcebergManifestEntry> delete_manifest_entries;

	//! Scanned data manifests and their owners.
	mutable vector<IcebergManifestListEntry> committed_data_manifests;
	mutable vector<reference<const IcebergManifestListEntry>> transaction_data_manifests;
	mutable unique_ptr<IcebergManifestScanningState> data_manifest_read_state;
	mutable unique_ptr<manifest_file::ManifestReader> data_manifest_reader;

	//! Declared after the manifest owners so references in parsed delete data are destroyed first.
	mutable case_insensitive_map_t<shared_ptr<IcebergDeleteData>> positional_delete_data;
	mutable map<sequence_number_t, unique_ptr<IcebergEqualityDeleteData>> equality_delete_data;

	//! Populated as parsed data-file entries become visible to any filtered view.
	mutable case_insensitive_map_t<vector<IcebergPartitionInfo>> data_file_partition_info;
};

struct IcebergDataViewCursor {
public:
	idx_t next_batch_idx = 0;
	bool has_current_batch = false;
	ManifestReadBatch current_batch;
	idx_t current_batch_offset = 0;
};

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
	const IcebergSnapshotScanInfo &GetSnapshot() const;
	const IcebergTableSchema &GetSchema() const;
	IcebergTableEntry *GetTable() const;
	const shared_ptr<IcebergScanInfo> &GetScanInfo() const;
	void SetTable(IcebergTableEntry *table);
	void SetOptions(const IcebergOptions &options);
	void PushTableFilter(column_t column_idx, unique_ptr<ExpressionFilter> filter);

	void Bind(vector<LogicalType> &return_types, vector<Identifier> &names);
	unique_ptr<IcebergMultiFileList> PushdownInternal(ClientContext &context, TableFilterSet &new_filters,
	                                                  const vector<column_t> &column_indexes) const;
	unique_ptr<DeleteFilter> GetPositionalDeletesForFile(const string &file_path) const;
	void ProcessDeletes(const vector<MultiFileColumnDefinition> &global_columns,
	                    const vector<ColumnIndex> &global_column_ids, const vector<idx_t> &projection_ids) const;
	vector<reference<const IcebergEqualityDeleteFile>>
	GetEqualityDeletesForFile(const BoundIcebergManifestEntry &manifest_entry) const;
	void GetStatistics(vector<PartitionStatistics> &result) const;
	BoundIcebergManifestEntry GetManifestEntry(idx_t file_id) const;
	vector<IcebergPartitionInfo> GetPartitionInfoForDataFile(const string &file_path) const;
	const IcebergManifestFile &GetManifestFileForEntry(const BoundIcebergManifestEntry &entry,
	                                                   IcebergManifestContentType type) const;
	vector<BoundIcebergManifestEntry> GetDeleteManifestEntries() const;
	shared_ptr<IcebergDeleteData> GetExistingPositionalDeleteData(const string &file_path) const;

public:
	//! MultiFileList API
	unique_ptr<MultiFileList> DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                const vector<Identifier> &names, const vector<LogicalType> &types,
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
	bool FileMatchesFilter(const IcebergManifestFile &manifest_file, const IcebergManifestEntry &manifest_entry,
	                       IcebergManifestContentType file_type) const;
	// TODO: How to guarantee we only call this after the filter pushdown?
	void InitializeFiles(lock_guard<mutex> &guard) const;

	//! NOTE: this requires the lock because it modifies the 'data_files' vector, potentially invalidating references
	optional_ptr<const BoundIcebergManifestEntry> GetDataFile(idx_t file_id, lock_guard<mutex> &guard) const;

	unique_ptr<ExpressionFilter> GetFilterForColumnIndex(const IcebergTableFilters &filter_set,
	                                                     const ColumnIndex &column_index) const;

private:
	IcebergMultiFileList(shared_ptr<IcebergMultiFileListSharedState> shared_state);
	bool TryGetNextBatch(lock_guard<mutex> &guard) const;
	void FinishScanTasks(lock_guard<mutex> &guard) const;
	void InitializeSharedState(lock_guard<mutex> &guard) const;
	bool FinishedScanningDeletes() const;
	void EnumerateDeleteManifestEntriesInternal() const;
	void ProcessDeletesInternal(const vector<MultiFileColumnDefinition> &global_columns,
	                            const vector<ColumnIndex> &global_column_ids,
	                            const vector<idx_t> &projection_ids) const;
	void ScanDeleteFiles(const vector<MultiFileColumnDefinition> &global_columns,
	                     const vector<ColumnIndex> &global_column_ids, const vector<idx_t> &projection_ids) const;
	void ScanDeleteFile(const BoundIcebergManifestEntry &entry, const vector<MultiFileColumnDefinition> &global_columns,
	                    const vector<ColumnIndex> &global_column_ids, const vector<idx_t> &projection_ids) const;
	void ScanPositionalDeleteFile(const BoundIcebergManifestEntry &manifest_entry, DataChunk &result) const;
	void ScanEqualityDeleteFile(const BoundIcebergManifestEntry &manifest_entry, DataChunk &result,
	                            vector<MultiFileColumnDefinition> &columns,
	                            const vector<MultiFileColumnDefinition> &global_columns,
	                            const vector<ColumnIndex> &global_column_ids,
	                            const vector<idx_t> &projection_ids) const;
	void ScanPuffinFile(const BoundIcebergManifestEntry &entry) const;

private:
	shared_ptr<IcebergMultiFileListSharedState> shared_state;
	ClientContext &context;
	FileSystem &fs;
	const IcebergOptions &options;
	//! ComplexFilterPushdown results
	bool have_bound = false;
	vector<string> names;
	vector<LogicalType> types;
	IcebergTableFilters table_filters;

	mutable bool view_initialized = false;
	mutable IcebergDataViewCursor data_view_cursor;
	//! Combination of committed + transaction delete manifests
	mutable vector<BoundIcebergManifestListEntry> delete_manifests;
	mutable vector<bool> delete_manifest_matches;

private:
	//! References to items inside the 'manifest_entries' of the list entries in the 'data_manifests'
	mutable vector<BoundIcebergManifestEntry> data_manifest_entries;
	//! Combination of committed + transaction data manifests
	mutable vector<BoundIcebergManifestListEntry> data_manifests;
	mutable vector<bool> data_manifest_matches;
};

} // namespace duckdb
