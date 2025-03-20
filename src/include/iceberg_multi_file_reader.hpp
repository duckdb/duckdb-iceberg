//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "manifest_reader.hpp"

namespace duckdb {

// struct IcebergFileMetaData {
// public:
//    IcebergFileMetaData() {};
//    IcebergFileMetaData (const IcebergFileMetaData&) = delete;
//    IcebergFileMetaData& operator= (const IcebergFileMetaData&) = delete;
// public:
//    optional_idx iceberg_snapshot_version;
//    optional_idx file_number;
//    optional_idx cardinality;
//    case_insensitive_map_t<string> partition_map;
//};

struct IcebergDeleteData {
public:
	IcebergDeleteData() {
	}

public:
	void AddRow(int64_t row_id) {
		temp_invalid_rows.insert(row_id);
	}

	void Apply(DataChunk &chunk, Vector &row_id_column) {
		D_ASSERT(row_id_column.GetType() == LogicalType::BIGINT);

		if (chunk.size() == 0) {
			return;
		}
		auto count = chunk.size();
		UnifiedVectorFormat data;
		row_id_column.ToUnifiedFormat(count, data);
		auto row_ids = UnifiedVectorFormat::GetData<int64_t>(data);

		SelectionVector result {count};
		idx_t selection_idx = 0;
		for (idx_t tuple_idx = 0; tuple_idx < count; tuple_idx++) {
			auto current_row_id = row_ids[data.sel->get_index(tuple_idx)];
			if (temp_invalid_rows.find(current_row_id) == temp_invalid_rows.end()) {
				result[selection_idx++] = tuple_idx;
			}
		}

		chunk.Slice(result, selection_idx);
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
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileReaderOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) override;
	vector<string> GetAllFiles() override;
	FileExpandResult GetExpandResult() override;
	idx_t GetTotalFileCount() override;
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context) override;

public:
	void ScanDeleteFile(const string &delete_file_path) const;
	optional_ptr<IcebergDeleteData> GetDeletesForFile(const string &file_path) const;
	void ProcessDeletes() const;

protected:
	//! Get the i-th expanded file
	string GetFile(idx_t i) override;
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
	mutable case_insensitive_map_t<IcebergDeleteData> delete_data;
	mutable mutex delete_lock;

	bool initialized = false;
	ClientContext &context;
	const IcebergOptions &options;
	IcebergSnapshot snapshot;
};

struct IcebergMultiFileReaderGlobalState : public MultiFileReaderGlobalState {
public:
	IcebergMultiFileReaderGlobalState(vector<LogicalType> extra_columns_p, const MultiFileList &file_list_p)
	    : MultiFileReaderGlobalState(std::move(extra_columns_p), file_list_p) {
	}

public:
	//! The index of the column in the chunk that relates to the file_row_number
	optional_idx file_row_number_idx;
};

struct IcebergMultiFileReader : public MultiFileReader {
public:
	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);
	//! Return a IcebergSnapshot
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         FileGlobOptions options) override;

	//! Override the regular parquet bind using the MultiFileReader Bind. The bind from these are what DuckDB's file
	//! readers will try read
	bool Bind(MultiFileReaderOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	          vector<string> &names, MultiFileReaderBindData &bind_data) override;

	//! Override the Options bind
	void BindOptions(MultiFileReaderOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<string> &names, MultiFileReaderBindData &bind_data) override;

	void CreateColumnMapping(const string &file_name,
	                                 const vector<MultiFileReaderColumnDefinition> &local_columns,
	                                 const vector<MultiFileReaderColumnDefinition> &global_columns,
	                                 const vector<ColumnIndex> &global_column_ids, MultiFileReaderData &reader_data,
	                                 const MultiFileReaderBindData &bind_data, const string &initial_file,
	                                 optional_ptr<MultiFileReaderGlobalState> global_state) override;

	unique_ptr<MultiFileReaderGlobalState>
	InitializeGlobalState(ClientContext &context, const MultiFileReaderOptions &file_options,
	                      const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
	                      const vector<MultiFileReaderColumnDefinition> &global_columns,
	                      const vector<ColumnIndex> &global_column_ids) override;

	void FinalizeBind(const MultiFileReaderOptions &file_options,
	                                     const MultiFileReaderBindData &options, const string &filename,
	                                     const vector<MultiFileReaderColumnDefinition> &local_columns,
	                                     const vector<MultiFileReaderColumnDefinition> &global_columns,
	                                     const vector<ColumnIndex> &global_column_ids, MultiFileReaderData &reader_data,
	                                     ClientContext &context, optional_ptr<MultiFileReaderGlobalState> global_state) override;

	//! Override the FinalizeChunk method
	void FinalizeChunk(ClientContext &context, const MultiFileReaderBindData &bind_data,
	                   const MultiFileReaderData &reader_data, DataChunk &chunk,
	                   optional_ptr<MultiFileReaderGlobalState> global_state) override;

	//! Override the ParseOption call to parse iceberg_scan specific options
	bool ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options,
	                 ClientContext &context) override;

public:
	IcebergOptions options;
};

} // namespace duckdb