#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "function/ducklake/ducklake_delete_file.hpp"
#include "function/ducklake/ducklake_data_file.hpp"
#include "function/ducklake/ducklake_column.hpp"
#include "function/ducklake/ducklake_snapshot.hpp"
#include "function/ducklake/ducklake_partition.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

struct DuckLakeSchema;

struct DuckLakeTable {
public:
	DuckLakeTable(const string &table_uuid, const string &table_name);

public:
	unordered_map<int64_t, reference<DuckLakeColumn>> GetColumnsAtSnapshot(DuckLakeSnapshot &snapshot);

public:
	//! ----- Column Updates -----

	//! Used for both ALTER and ADD
	void AddColumnVersion(DuckLakeColumn &new_column, DuckLakeSnapshot &begin_snapshot);
	void DropColumnVersion(int64_t column_id, DuckLakeSnapshot &end_snapshot);

public:
	//! ----- Partition Updates -----

	DuckLakePartition &AddPartition(unique_ptr<DuckLakePartition> new_partition, DuckLakeSnapshot &begin_snapshot);

public:
	//! ----- Data File Updates -----

	void AddDataFile(DuckLakeDataFile &data_file, DuckLakeSnapshot &begin_snapshot);
	void DeleteDataFile(const string &data_file_path, DuckLakeSnapshot &end_snapshot);

public:
	//! ----- Delete File Updates -----

	void AddDeleteFile(DuckLakeDeleteFile &delete_file, DuckLakeSnapshot &begin_snapshot);
	void DeleteDeleteFile(const string &delete_file_path, DuckLakeSnapshot &end_snapshot);

public:
	string FinalizeEntry(int64_t schema_id, const map<timestamp_t, DuckLakeSnapshot> &snapshots);

public:
	string table_uuid;
	//! FIXME: we don't support renames of tables, but then again, I don't think we can
	string table_name;
	string schema_name;

	bool has_snapshot = false;
	timestamp_t start_snapshot;

	//! The table id is assigned after we've processed all tables
	optional_idx table_id;
	idx_t catalog_id_offset = DConstants::INVALID_INDEX;

	vector<DuckLakeColumn> all_columns;
	unordered_map<int64_t, idx_t> current_columns;

	vector<unique_ptr<DuckLakePartition>> all_partitions;
	optional_ptr<DuckLakePartition> current_partition;

	vector<DuckLakeDataFile> all_data_files;
	unordered_map<string, idx_t> current_data_files;

	vector<DuckLakeDeleteFile> all_delete_files;
	unordered_map<string, idx_t> current_delete_files;

	//! Keep track of which data files are referenced by active delete files
	unordered_set<string> referenced_data_files;
};

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
