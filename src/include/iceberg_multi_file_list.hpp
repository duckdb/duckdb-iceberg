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
#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "manifest_reader.hpp"

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/list.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

struct IcebergEqualityDeleteRow {
public:
	IcebergEqualityDeleteRow() {
	}
	IcebergEqualityDeleteRow(const IcebergEqualityDeleteRow &) = delete;
	IcebergEqualityDeleteRow &operator=(const IcebergEqualityDeleteRow &) = delete;
	IcebergEqualityDeleteRow(IcebergEqualityDeleteRow &&) = default;
	IcebergEqualityDeleteRow &operator=(IcebergEqualityDeleteRow &&) = default;

public:
	//! Map of field-id to equality delete for the field
	//! NOTE: these are either OPERATOR_IS_NULL or COMPARE_EQUAL
	//! Also note: it's probably easiest to apply these to the 'output_chunk' of FinalizeChunk, so we can re-use
	//! expressions. Otherwise the idx of the BoundReferenceExpression would have to change for every file.
	unordered_map<int32_t, unique_ptr<Expression>> filters;
};

struct IcebergEqualityDeleteFile {
public:
	IcebergEqualityDeleteFile(Value partition, int32_t partition_spec_id)
	    : partition(partition), partition_spec_id(partition_spec_id) {
	}

public:
	//! The partition value (struct) if the equality delete has partition information
	Value partition;
	int32_t partition_spec_id;
	vector<IcebergEqualityDeleteRow> rows;
};

struct IcebergEqualityDeleteData {
public:
	IcebergEqualityDeleteData(sequence_number_t sequence_number) : sequence_number(sequence_number) {
	}

public:
	sequence_number_t sequence_number;
	vector<IcebergEqualityDeleteFile> files;
};

struct IcebergPositionalDeleteData : public DeleteFilter {
public:
	IcebergPositionalDeleteData() {
	}

public:
	void AddRow(int64_t row_id) {
		temp_invalid_rows.insert(row_id);
	}

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override {
		if (count == 0) {
			return 0;
		}
		result_sel.Initialize(STANDARD_VECTOR_SIZE);
		idx_t selection_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			if (!temp_invalid_rows.count(i + start_row_index)) {
				result_sel.set_index(selection_idx++, i);
			}
		}
		return selection_idx;
	}

public:
	//! Store invalid rows here before finalizing into a SelectionVector
	unordered_set<int64_t> temp_invalid_rows;
};

struct IcebergMultiFileList : public MultiFileList {
public:
	IcebergMultiFileList(ClientContext &context, shared_ptr<IcebergScanInfo> scan_info, const string &path,
	                     const IcebergOptions &options);

public:
	static string ToDuckDBPath(const string &raw_path);
	string GetPath() const;
	const IcebergTableMetadata &GetMetadata() const;
	optional_ptr<IcebergSnapshot> GetSnapshot() const;
	const IcebergTableSchema &GetSchema() const;

	void Bind(vector<LogicalType> &return_types, vector<string> &names);
	unique_ptr<IcebergMultiFileList> PushdownInternal(ClientContext &context, TableFilterSet &new_filters) const;
	void ScanPositionalDeleteFile(DataChunk &result) const;
	void ScanEqualityDeleteFile(const IcebergManifestEntry &entry, DataChunk &result,
	                            vector<MultiFileColumnDefinition> &columns,
	                            const vector<MultiFileColumnDefinition> &global_columns,
	                            const vector<ColumnIndex> &column_indexes) const;
	void ScanDeleteFile(const IcebergManifestEntry &entry, const vector<MultiFileColumnDefinition> &global_columns,
	                    const vector<ColumnIndex> &column_indexes) const;
	unique_ptr<IcebergPositionalDeleteData> GetPositionalDeletesForFile(const string &file_path) const;
	void ProcessDeletes(const vector<MultiFileColumnDefinition> &global_columns,
	                    const vector<ColumnIndex> &column_indexes) const;

public:
	//! MultiFileList API
	unique_ptr<MultiFileList> DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                const vector<string> &names, const vector<LogicalType> &types,
	                                                const vector<column_t> &column_ids,
	                                                TableFilterSet &filters) const override;
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) override;
	vector<OpenFileInfo> GetAllFiles() override;
	FileExpandResult GetExpandResult() override;
	idx_t GetTotalFileCount() override;
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context) override;

protected:
	//! Get the i-th expanded file
	OpenFileInfo GetFile(idx_t i) override;
	OpenFileInfo GetFileInternal(idx_t i, lock_guard<mutex> &guard);

protected:
	bool ManifestMatchesFilter(const IcebergManifest &manifest);
	bool FileMatchesFilter(const IcebergManifestEntry &file);
	// TODO: How to guarantee we only call this after the filter pushdown?
	void InitializeFiles(lock_guard<mutex> &guard);

	//! NOTE: this requires the lock because it modifies the 'data_files' vector, potentially invalidating references
	optional_ptr<const IcebergManifestEntry> GetDataFile(idx_t file_id, lock_guard<mutex> &guard);

public:
	ClientContext &context;
	FileSystem &fs;
	shared_ptr<IcebergScanInfo> scan_info;
	string path;

	mutable mutex lock;
	//! ComplexFilterPushdown results
	bool have_bound = false;
	vector<string> names;
	vector<LogicalType> types;
	TableFilterSet table_filters;

	unique_ptr<manifest_file::ManifestFileReader> data_manifest_reader;
	unique_ptr<manifest_file::ManifestFileReader> delete_manifest_reader;

	vector<IcebergManifestEntry> data_files;
	vector<IcebergManifest> data_manifests;
	vector<IcebergManifest> delete_manifests;

	vector<IcebergManifest>::iterator current_data_manifest;
	mutable vector<IcebergManifest>::iterator current_delete_manifest;
	//! The data files of the manifest file that we last scanned
	idx_t data_file_idx = 0;
	vector<IcebergManifestEntry> current_data_files;

	//! For each file that has a delete file, the state for processing that/those delete file(s)
	mutable case_insensitive_map_t<unique_ptr<IcebergPositionalDeleteData>> positional_delete_data;
	//! All equality deletes with sequence numbers higher than that of the data_file apply to that data_file
	mutable map<sequence_number_t, unique_ptr<IcebergEqualityDeleteData>> equality_delete_data;
	mutable mutex delete_lock;

	bool initialized = false;
	const IcebergOptions &options;
};

} // namespace duckdb
