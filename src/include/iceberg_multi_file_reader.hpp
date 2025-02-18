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

	void Finalize() {
		D_ASSERT(!temp_invalid_rows.empty());
		idx_t selection_vector_size = *temp_invalid_rows.rbegin();
		idx_t needed_space = selection_vector_size - (temp_invalid_rows.size() - 1);
		valid_rows.Initialize(needed_space);

		idx_t row_id = 0;
		idx_t size = 0;
		for (auto &invalid_row : temp_invalid_rows) {
			while (row_id < invalid_row) {
				valid_rows[size++] = row_id++;
			}
			row_id = invalid_row + 1;
		}
		D_ASSERT(size == needed_space);
		total_count = size;
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

		auto starting_row_id = row_ids[data.sel->get_index(0)];
		idx_t valid_idx = 0;
		for (; valid_idx < total_count; valid_idx++) {
			if (valid_rows[valid_idx] >= starting_row_id) {
				break;
			}
		}
		//if (valid_idx + count < total_count && valid_rows[valid_idx + count] == starting_row_id + count) {
		//	//! Everything in the chunk is valid, early out
		//	return;
		//}

		SelectionVector result {count};
		idx_t selection_idx = 0;
		for (idx_t i = 0; i < count && valid_idx < total_count; i++) {
			if (starting_row_id + i == valid_rows[valid_idx]) {
				result[selection_idx++] = i;
				valid_idx++;
			}
		}

		if (valid_idx >= total_count && !temp_invalid_rows.empty() && starting_row_id + count > *temp_invalid_rows.rbegin()) {
			//! The deletes have only told us what the highest deleted row is
			//! But anything after that is valid and can't be ignored.
			auto last_invalid_row = *temp_invalid_rows.rbegin();
			auto valid_range = (starting_row_id + count) - (last_invalid_row + 1);
			auto i = count - valid_range;

			for (; i < count; i++) {
				result[selection_idx++] = i;
			}
		}

		chunk.Slice(result, selection_idx);
	}

public:
	//! Store invalid rows here before finalizing into a SelectionVector
	set<int64_t> temp_invalid_rows;

	//! The selection of rows that are not deleted
	SelectionVector valid_rows;
	idx_t highest_invalid_row;
	idx_t total_count = 0;
};

struct IcebergMultiFileList : public MultiFileList {
public:
	IcebergMultiFileList(ClientContext &context, const string &path, IcebergOptions options);

public:
	static string ToDuckDBPath(const string &raw_path);
	string GetPath();

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

	////! Metadata map for files
	// vector<unique_ptr<IcebergFileMetaData>> metadata;

	////! Current file list resolution state
	unique_ptr<ManifestReader> manifest_reader;
	unique_ptr<ManifestEntryReader> data_manifest_entry_reader;
	unique_ptr<ManifestEntryReader> delete_manifest_entry_reader;
	ManifestEntryReaderState reader_state;
	vector<IcebergManifestEntry> data_files;

	vector<unique_ptr<IcebergManifest>> data_manifests;
	vector<unique_ptr<IcebergManifest>> delete_manifests;
	vector<unique_ptr<IcebergManifest>>::iterator current_data_manifest;
	mutable vector<unique_ptr<IcebergManifest>>::iterator current_delete_manifest;

	//! For each file that has a delete file, the state for processing that/those delete file(s)
	mutable case_insensitive_map_t<IcebergDeleteData> delete_data;
	mutable mutex delete_lock;

	bool initialized = false;
	ClientContext &context;
	IcebergOptions options;
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