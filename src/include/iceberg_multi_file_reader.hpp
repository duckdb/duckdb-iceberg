//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "manifest_reader.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"

namespace duckdb {

struct IcebergDeleteData : public DeleteFilter {
public:
	IcebergDeleteData() {
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
	IcebergMultiFileList(ClientContext &context, const string &path, const IcebergOptions &options);

public:
	static string ToDuckDBPath(const string &raw_path);
	string GetPath() const;

public:
	//! MultiFileList API
	void Bind(vector<LogicalType> &return_types, vector<string> &names);
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) override;
	vector<OpenFileInfo> GetAllFiles() override;
	FileExpandResult GetExpandResult() override;
	idx_t GetTotalFileCount() override;
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context) override;

public:
	void ScanDeleteFile(const string &delete_file_path) const;
	unique_ptr<IcebergDeleteData> GetDeletesForFile(const string &file_path) const;
	void ProcessDeletes() const;

protected:
	//! Get the i-th expanded file
	OpenFileInfo GetFile(idx_t i) override;

	// TODO: How to guarantee we only call this after the filter pushdown?
	void InitializeFiles();

public:
	mutex lock;
	// idx_t version;

	//! ComplexFilterPushdown results
	vector<string> names;
	TableFilterSet table_filters;

	unique_ptr<ManifestReader> manifest_reader;
	unique_ptr<ManifestReader> data_manifest_entry_reader;
	unique_ptr<ManifestReader> delete_manifest_entry_reader;

	manifest_reader_manifest_producer manifest_producer = nullptr;
	manifest_reader_manifest_entry_producer entry_producer = nullptr;

	vector<IcebergManifestEntry> data_files;
	vector<IcebergManifest> data_manifests;
	vector<IcebergManifest> delete_manifests;
	vector<IcebergManifest>::iterator current_data_manifest;
	mutable vector<IcebergManifest>::iterator current_delete_manifest;

	//! For each file that has a delete file, the state for processing that/those delete file(s)
	mutable case_insensitive_map_t<unique_ptr<IcebergDeleteData>> delete_data;
	mutable mutex delete_lock;

	bool initialized = false;
	ClientContext &context;
	const IcebergOptions &options;
	unique_ptr<IcebergMetadata> metadata;
	IcebergSnapshot snapshot;
};

struct IcebergMultiFileReaderGlobalState : public MultiFileReaderGlobalState {
public:
	IcebergMultiFileReaderGlobalState(vector<LogicalType> extra_columns_p, const MultiFileList &file_list_p)
	    : MultiFileReaderGlobalState(std::move(extra_columns_p), file_list_p) {
	}
};

struct IcebergMultiFileReader : public MultiFileReader {
public:
	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);
	//! Return a IcebergSnapshot
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         FileGlobOptions options) override;

	//! Override the regular parquet bind using the MultiFileReader Bind. The bind from these are what DuckDB's file
	//! readers will try read
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types, vector<string> &names,
	          MultiFileReaderBindData &bind_data) override;

	//! Override the Options bind
	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<string> &names, MultiFileReaderBindData &bind_data) override;

	unique_ptr<MultiFileReaderGlobalState>
	InitializeGlobalState(ClientContext &context, const MultiFileOptions &file_options,
	                      const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
	                      const vector<MultiFileColumnDefinition> &global_columns,
	                      const vector<ColumnIndex> &global_column_ids) override;

	void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
	                  const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
	                  const vector<ColumnIndex> &global_column_ids, ClientContext &context,
	                  optional_ptr<MultiFileReaderGlobalState> global_state) override;

	//! Override the FinalizeChunk method
	void FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data, BaseFileReader &reader,
	                   const MultiFileReaderData &reader_data, DataChunk &input_chunk, DataChunk &output_chunk,
	                   ExpressionExecutor &executor, optional_ptr<MultiFileReaderGlobalState> global_state) override;

	//! Override the ParseOption call to parse iceberg_scan specific options
	bool ParseOption(const string &key, const Value &val, MultiFileOptions &options, ClientContext &context) override;

public:
	IcebergOptions options;
};

} // namespace duckdb
