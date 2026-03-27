#include "function/ducklake/ducklake_data_file.hpp"
#include "function/ducklake/ducklake_utils.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

DuckLakeDataFile::DuckLakeDataFile(const IcebergManifestEntry &manifest_entry_p, DuckLakePartition &partition,
                                   const string &table_name)
    : manifest_entry(manifest_entry_p), partition(partition) {
	auto &data_file = manifest_entry.data_file;
	path = data_file.file_path;
	if (!StringUtil::CIEquals(data_file.file_format, "parquet")) {
		throw InvalidInputException("Can't convert Iceberg table (name: %s) to DuckLake, because it contains a "
		                            "data file with file_format '%s'",
		                            table_name, data_file.file_format);
	}
	record_count = data_file.record_count;
	file_size_bytes = data_file.file_size_in_bytes;
}

string DuckLakeDataFile::FinalizeEntry(int64_t table_id, const map<timestamp_t, DuckLakeSnapshot> &snapshots) {
	//! NOTE: partitions have to be finalized before data files
	D_ASSERT(partition.partition_id.IsValid());
	auto &snapshot = snapshots.at(start_snapshot);
	int64_t data_file_id = snapshot.base_file_id + file_id_offset;
	this->data_file_id = data_file_id;

	auto snapshot_ids = DuckLakeUtils::GetSnapshots(start_snapshot, has_end, end_snapshot, snapshots);
	int64_t partition_id = partition.partition_id.GetIndex();

	return StringUtil::Format(
	    "VALUES(%d, %d, %d, %s, NULL, '%s', False, 'parquet', %d, %d, NULL, NULL, %d, NULL, NULL, NULL);", data_file_id,
	    table_id, snapshot_ids.first, snapshot_ids.second, path, record_count, file_size_bytes, partition_id);
}

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
