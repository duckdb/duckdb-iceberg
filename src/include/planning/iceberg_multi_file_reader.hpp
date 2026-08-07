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
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "planning/iceberg_multi_file_list.hpp"
#include "common/iceberg_utils.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

namespace duckdb {

struct IcebergEqualityDeleteReadColumn {
	int32_t field_id;
	idx_t expression_index;
	LogicalType type;
};

struct IcebergEqualityDeleteReadState {
	explicit IcebergEqualityDeleteReadState(vector<IcebergEqualityDeleteReadColumn> columns_p)
	    : columns(std::move(columns_p)) {
		for (idx_t i = 0; i < columns.size(); i++) {
			field_indexes.emplace(columns[i].field_id, i);
			types.push_back(columns[i].type);
		}
	}

	vector<IcebergEqualityDeleteReadColumn> columns;
	vector<LogicalType> types;
	unordered_map<int32_t, idx_t> field_indexes;
	unique_ptr<Expression> expression;
};

struct IcebergMultiFileReaderGlobalState : public MultiFileReaderGlobalState {
public:
	explicit IcebergMultiFileReaderGlobalState(const MultiFileList &file_list_p)
	    : MultiFileReaderGlobalState({}, file_list_p, true) {
	}

	void CacheEqualityDeleteReadState(idx_t file_list_idx, unique_ptr<IcebergEqualityDeleteReadState> read_state) {
		lock_guard<mutex> guard(equality_delete_read_state_lock);
		auto &cached_state = equality_delete_read_states[file_list_idx];
		if (cached_state) {
			throw InternalException("Equality-delete state was initialized twice for file-list index %llu",
			                        file_list_idx);
		}
		cached_state = std::move(read_state);
	}

	const IcebergEqualityDeleteReadState &GetEqualityDeleteReadState(idx_t file_list_idx) const {
		lock_guard<mutex> guard(equality_delete_read_state_lock);
		auto entry = equality_delete_read_states.find(file_list_idx);
		if (entry == equality_delete_read_states.end()) {
			throw InternalException("Equality-delete state was not initialized for file-list index %llu",
			                        file_list_idx);
		}
		return *entry->second;
	}

private:
	mutable mutex equality_delete_read_state_lock;
	//! The values are heap allocated so references remain stable while other files are initialized in parallel.
	unordered_map<idx_t, unique_ptr<IcebergEqualityDeleteReadState>> equality_delete_read_states;
};

struct IcebergMultiFileReader : public MultiFileReader {
public:
	static constexpr column_t COLUMN_IDENTIFIER_LAST_SEQUENCE_NUMBER = UINT64_C(10000000000000000000);

public:
	IcebergMultiFileReader(shared_ptr<TableFunctionInfo> function_info);

public:
	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);
	static vector<PartitionStatistics> IcebergGetPartitionStats(ClientContext &context, GetPartitionStatsInput &input);

public:
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	          vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;
	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;
	unique_ptr<MultiFileReaderGlobalState>
	InitializeGlobalState(ClientContext &context, const MultiFileOptions &file_options,
	                      const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
	                      const vector<MultiFileColumnDefinition> &global_columns,
	                      const vector<ColumnIndex> &global_column_ids) override;
	ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind_data,
	                                      const vector<MultiFileColumnDefinition> &global_columns,
	                                      const vector<ColumnIndex> &global_column_ids,
	                                      optional_ptr<TableFilterSet> table_filters, ClientContext &context,
	                                      MultiFileGlobalState &gstate) override;
	void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
	                  const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
	                  const vector<ColumnIndex> &global_column_ids, ClientContext &context,
	                  optional_ptr<MultiFileReaderGlobalState> global_state) override;
	void FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data, BaseFileReader &reader,
	                   const MultiFileReaderData &reader_data, DataChunk &input_chunk, DataChunk &output_chunk,
	                   ExpressionExecutor &executor, optional_ptr<MultiFileReaderGlobalState> global_state) override;
	bool ParseOption(const Identifier &key, const Value &val, MultiFileOptions &options,
	                 ClientContext &context) override;

	MultiFileReaderVirtualColumnBinding
	GetVirtualColumnExpression(ClientContext &context, MultiFileReaderData &reader_data,
	                           const vector<MultiFileColumnDefinition> &local_columns, const idx_t column_id,
	                           const LogicalType &type, MultiFileLocalIndex local_idx) override;

private:
	static unique_ptr<Expression>
	CreateEqualityDeleteExpression(const vector<reference<const IcebergEqualityDeleteFile>> &delete_files,
	                               const vector<MultiFileColumnDefinition> &local_columns,
	                               const IcebergEqualityDeleteReadState &read_state);
	static vector<IcebergEqualityDeleteReadColumn>
	AddEqualityDeleteColumns(const IcebergTableMetadata &metadata,
	                         const vector<reference<const IcebergEqualityDeleteFile>> &delete_files,
	                         vector<MultiFileColumnDefinition> &scan_columns, vector<ColumnIndex> &scan_column_ids,
	                         MultiFileReaderData &reader_data, ClientContext &context);
	static IcebergEqualityDeleteReadColumn
	AddEqualityDeleteColumn(const IcebergTableMetadata &metadata, int32_t field_id,
	                        vector<MultiFileColumnDefinition> &scan_columns, vector<ColumnIndex> &scan_column_ids,
	                        MultiFileReaderData &reader_data, ClientContext &context);
	static void ApplyPartitionConstants(const IcebergManifestFile &manifest_file,
	                                    const BoundIcebergManifestEntry &bound_manifest_entry,
	                                    const IcebergTableMetadata &metadata, MultiFileReaderData &reader_data,
	                                    const vector<MultiFileColumnDefinition> &global_columns,
	                                    const vector<ColumnIndex> &global_column_ids, ClientContext &context);

public:
	shared_ptr<TableFunctionInfo> function_info;
	IcebergOptions options;

private:
	unique_ptr<MultiFileColumnDefinition> row_id_column;
	unique_ptr<MultiFileColumnDefinition> last_updated_sequence_number_column;
};

} // namespace duckdb
