//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/iceberg_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/iceberg_metadata_info.hpp"
#include "storage/iceberg_delete_filter.hpp"
#include "storage/irc_table_entry.hpp"
#include "storage/irc_schema_entry.hpp"

namespace duckdb {

struct IcebergDeleteMap {

	void AddExtendedFileInfo(IcebergFileListExtendedEntry file_entry) {
		auto filename = file_entry.file.path;
		file_map.emplace(std::move(filename), std::move(file_entry));
	}

	IcebergFileListExtendedEntry GetExtendedFileInfo(const string &filename) {
		auto delete_entry = file_map.find(filename);
		if (delete_entry == file_map.end()) {
			throw InternalException("Could not find matching file for written delete file");
		}
		return delete_entry->second;
	}

	optional_ptr<IcebergDeleteData> GetDeleteData(const string &filename) {
		lock_guard<mutex> guard(lock);
		auto entry = delete_data_map.find(filename);
		if (entry == delete_data_map.end()) {
			return nullptr;
		}
		return entry->second.get();
	}

	void ClearDeletes(const string &filename) {
		lock_guard<mutex> guard(lock);
		delete_data_map.erase(filename);
	}

	void AddDeleteData(const string &filename, shared_ptr<IcebergDeleteData> delete_data) {
		lock_guard<mutex> guard(lock);
		delete_data_map.emplace(filename, std::move(delete_data));
	}

private:
	mutex lock;
	unordered_map<string, IcebergFileListExtendedEntry> file_map;
	unordered_map<string, shared_ptr<IcebergDeleteData>> delete_data_map;
};

struct WrittenColumnInfo {
	WrittenColumnInfo() = default;
	WrittenColumnInfo(LogicalType type_p, int32_t field_id) : type(std::move(type_p)), field_id(field_id) {
	}

	LogicalType type;
	int32_t field_id;
};

class IcebergDeleteLocalState : public LocalSinkState {
public:
	optional_idx current_file_index;
	vector<idx_t> file_row_numbers;
	unordered_map<idx_t, string> filenames;
};

class IcebergDeleteGlobalState : public GlobalSinkState {
public:
	explicit IcebergDeleteGlobalState() {
		written_columns["file_path"] = WrittenColumnInfo(LogicalType::VARCHAR, MultiFileReader::FILENAME_FIELD_ID);
		written_columns["pos"] = WrittenColumnInfo(LogicalType::BIGINT, MultiFileReader::ORDINAL_FIELD_ID);
	}

	mutex lock;
	unordered_map<string, IcebergDeleteFileInfo> written_files;
	unordered_map<string, WrittenColumnInfo> written_columns;
	idx_t total_deleted_count = 0;
	unordered_map<uint64_t, vector<idx_t>> deleted_rows;
	unordered_map<idx_t, string> filenames;

	void Flush(IcebergDeleteLocalState &local_state) {
		auto &local_entry = local_state.file_row_numbers;
		if (local_entry.empty()) {
			return;
		}
		lock_guard<mutex> guard(lock);
		auto &global_entry = deleted_rows[local_state.current_file_index.GetIndex()];
		global_entry.insert(global_entry.end(), local_entry.begin(), local_entry.end());
		total_deleted_count += local_entry.size();
		local_entry.clear();
	}

	void FinalFlush(IcebergDeleteLocalState &local_state) {
		Flush(local_state);
		// flush the file names to the global state
		lock_guard<mutex> guard(lock);
		for (auto &entry : local_state.filenames) {
			filenames.emplace(entry.first, entry.second);
		}
	}
};

class IcebergDelete : public PhysicalOperator {
public:
	IcebergDelete(PhysicalPlan &physical_plan, ICTableEntry &table, PhysicalOperator &child,
	              shared_ptr<IcebergDeleteMap> delete_map_p, vector<idx_t> row_id_indexes);

	//! The table to delete from
	ICTableEntry &table;
	// A map of filename -> data file index and filename -> delete data
	shared_ptr<IcebergDeleteMap> delete_map;
	//! The column indexes for the relevant row-id columns
	vector<idx_t> row_id_indexes;

public:
	// // Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	static PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, ICTableEntry &table,
	                                    PhysicalOperator &child_plan, vector<idx_t> row_id_indexes);

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	void FlushDelete(IRCTransaction &transaction, ClientContext &context, IcebergDeleteGlobalState &global_state,
	                 const string &filename, vector<idx_t> &deleted_rows) const;
};

} // namespace duckdb
