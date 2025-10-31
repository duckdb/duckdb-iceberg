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

struct WrittenColumnInfo {
	WrittenColumnInfo() = default;
	WrittenColumnInfo(LogicalType type_p, int32_t field_id) : type(std::move(type_p)), field_id(field_id) {
	}

	LogicalType type;
	int32_t field_id;
};

class IcebergDeleteLocalState : public LocalSinkState {
public:
	string current_file_name;
	vector<idx_t> file_row_numbers;
};

class IcebergDeleteGlobalState : public GlobalSinkState {
public:
	explicit IcebergDeleteGlobalState() {
		written_columns["file_path"] = WrittenColumnInfo(LogicalType::VARCHAR, MultiFileReader::FILENAME_FIELD_ID);
		written_columns["pos"] = WrittenColumnInfo(LogicalType::BIGINT, MultiFileReader::ORDINAL_FIELD_ID);
		total_deleted_count = 0;
	}

	mutex lock;
	unordered_map<string, IcebergDeleteFileInfo> written_files;
	unordered_map<string, WrittenColumnInfo> written_columns;
	atomic<idx_t> total_deleted_count;
	// data file name -> newly deleted rows.
	unordered_map<string, vector<idx_t>> deleted_rows;

	void Flush(IcebergDeleteLocalState &local_state) {
		auto &local_entry = local_state.file_row_numbers;
		if (local_entry.empty()) {
			return;
		}
		lock_guard<mutex> guard(lock);
		auto &global_entry = deleted_rows[local_state.current_file_name];
		global_entry.insert(global_entry.end(), local_entry.begin(), local_entry.end());
		total_deleted_count += local_entry.size();
		local_entry.clear();
	}

	void FinalFlush(IcebergDeleteLocalState &local_state) {
		Flush(local_state);
	}
};

class IcebergDelete : public PhysicalOperator {
public:
	IcebergDelete(PhysicalPlan &physical_plan, ICTableEntry &table, PhysicalOperator &child,
	              vector<idx_t> row_id_indexes);

	//! The table to delete from
	ICTableEntry &table;
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
	static vector<IcebergManifestEntry> GenerateDeleteManifestEntries(IcebergDeleteGlobalState &global_state);

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	void FlushDeletes(IRCTransaction &transaction, ClientContext &context,
	                  IcebergDeleteGlobalState &global_state) const;

private:
	void WritePositionalDeleteFile(ClientContext &context, IcebergDeleteGlobalState &global_state,
	                               const string &filename, IcebergDeleteFileInfo delete_file,
	                               set<idx_t> sorted_deletes) const;
};

} // namespace duckdb
