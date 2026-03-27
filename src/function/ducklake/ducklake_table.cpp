#include "function/ducklake/ducklake_table.hpp"
#include "function/ducklake/ducklake_utils.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

DuckLakeTable::DuckLakeTable(const string &table_uuid, const string &table_name)
    : table_uuid(table_uuid), table_name(table_name) {
}

unordered_map<int64_t, reference<DuckLakeColumn>> DuckLakeTable::GetColumnsAtSnapshot(DuckLakeSnapshot &snapshot) {
	unordered_map<int64_t, reference<DuckLakeColumn>> result;
	//! These conditions have to be true: begin <= snapshot AND end > snapshot

	for (auto &column : all_columns) {
		if (column.start_snapshot > snapshot.snapshot_time) {
			//! This column version was created after this snapshot
			continue;
		}
		if (column.has_end && column.end_snapshot <= snapshot.snapshot_time) {
			//! This column is deleted and it was deleted before our snapshot
			continue;
		}
		auto res = result.emplace(column.column_id, column);
		if (!res.second) {
			throw InvalidInputException(
			    "Iceberg integrity error: two columns with the same source id exist at the same time");
		}
	}
	return result;
}

void DuckLakeTable::AddColumnVersion(DuckLakeColumn &new_column, DuckLakeSnapshot &begin_snapshot) {
	//! First set the end snapshot of the old version of this column (if it exists)
	auto column_id = new_column.column_id;
	DropColumnVersion(column_id, begin_snapshot);
	begin_snapshot.AlterTable(table_uuid);
	new_column.start_snapshot = begin_snapshot.snapshot_time;
	all_columns.push_back(new_column);
	current_columns.emplace(column_id, all_columns.size() - 1);
}

void DuckLakeTable::DropColumnVersion(int64_t column_id, DuckLakeSnapshot &end_snapshot) {
	auto it = current_columns.find(column_id);
	if (it == current_columns.end()) {
		return;
	}
	auto &column = all_columns[it->second];
	end_snapshot.AlterTable(table_uuid);
	column.end_snapshot = end_snapshot.snapshot_time;
	column.has_end = true;
	current_columns.erase(it);
}

DuckLakePartition &DuckLakeTable::AddPartition(unique_ptr<DuckLakePartition> new_partition,
                                               DuckLakeSnapshot &begin_snapshot) {
	if (current_partition) {
		auto &old_partition = *current_partition;
		old_partition.end_snapshot = begin_snapshot.snapshot_time;
		old_partition.has_end = true;
		current_partition = nullptr;
	}
	begin_snapshot.AlterTable(table_uuid);
	new_partition->start_snapshot = begin_snapshot.snapshot_time;
	all_partitions.push_back(std::move(new_partition));
	current_partition = all_partitions.back().get();

	return *current_partition;
}

void DuckLakeTable::AddDataFile(DuckLakeDataFile &data_file, DuckLakeSnapshot &begin_snapshot) {
	data_file.start_snapshot = begin_snapshot.snapshot_time;
	data_file.file_id_offset = begin_snapshot.AddDataFile(table_uuid);
	D_ASSERT(!current_data_files.count(data_file.path));
	all_data_files.push_back(data_file);
	current_data_files.emplace(data_file.path, all_data_files.size() - 1);
}

void DuckLakeTable::DeleteDataFile(const string &data_file_path, DuckLakeSnapshot &end_snapshot) {
	auto it = current_data_files.find(data_file_path);
	if (it == current_data_files.end()) {
		//! Entries can be marked deleted without having an add, if they were added in the same snapshot
		//! This can be done to postpone deletion of created parquet files to a cleanup operation
		//! I don't know why you would want to do that.. but it's possible
		return;
	}

	auto &data_file = all_data_files[it->second];
	end_snapshot.DeleteDataFile(table_uuid);
	data_file.end_snapshot = end_snapshot.snapshot_time;
	data_file.has_end = true;

	current_data_files.erase(it);
}

void DuckLakeTable::AddDeleteFile(DuckLakeDeleteFile &delete_file, DuckLakeSnapshot &begin_snapshot) {
	delete_file.start_snapshot = begin_snapshot.snapshot_time;
	delete_file.file_id_offset = begin_snapshot.AddDeleteFile(table_uuid);
	D_ASSERT(!current_delete_files.count(delete_file.path));

	//! NOTE: because delete files reference data files by id, the Data Files have to be processed first.
	//! Find the referenced data file, which verifies that the file exists as well
	auto data_file_it = current_data_files.find(delete_file.data_file_path);
	if (data_file_it == current_data_files.end()) {
		throw InvalidInputException("Iceberg integrity error: Referencing a data file that doesn't exist?");
	}
	delete_file.referenced_data_file = data_file_it->second;

	//! Add to the set of referenced data files, verify that there is no other active delete file that references
	//! this data file.
	if (referenced_data_files.count(delete_file.data_file_path)) {
		throw InvalidInputException(
		    "Can't convert an Iceberg table (name: %s, uuid: %s) that has multiple deletes referencing the same "
		    "data file to a DuckLake table",
		    table_name, table_uuid);
	}
	referenced_data_files.insert(delete_file.data_file_path);

	all_delete_files.push_back(delete_file);
	current_delete_files.emplace(delete_file.path, all_delete_files.size() - 1);
}

void DuckLakeTable::DeleteDeleteFile(const string &delete_file_path, DuckLakeSnapshot &end_snapshot) {
	auto it = current_delete_files.find(delete_file_path);
	if (it == current_delete_files.end()) {
		throw InvalidInputException("Iceberg integrity error: Deleting a Delete File that doesn't exist?");
	}

	auto &delete_file = all_delete_files[it->second];
	//! end_snapshot.DeleteDeleteFile(table_uuid);
	delete_file.end_snapshot = end_snapshot.snapshot_time;
	delete_file.has_end = true;
	referenced_data_files.erase(delete_file.data_file_path);

	current_delete_files.erase(it);
}

string DuckLakeTable::FinalizeEntry(int64_t schema_id, const map<timestamp_t, DuckLakeSnapshot> &snapshots) {
	D_ASSERT(has_snapshot);
	auto &snapshot = snapshots.at(start_snapshot);
	auto table_id = snapshot.base_catalog_id + catalog_id_offset;
	this->table_id = table_id;

	auto snapshot_ids = DuckLakeUtils::GetSnapshots(start_snapshot, false, timestamp_t(0), snapshots);
	return StringUtil::Format("VALUES(%d, '%s', %d, %s, %d, '%s', '', false);", table_id, table_uuid,
	                          snapshot_ids.first, snapshot_ids.second, schema_id, table_name);
}

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
