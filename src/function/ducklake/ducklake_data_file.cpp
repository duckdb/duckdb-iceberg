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

	const auto DATA_FILE_SQL = R"(
		INSERT INTO {METADATA_CATALOG}.ducklake_data_file VALUES(
			%d, -- data_file_id
			%d, -- table_id
			%d, -- begin_snapshot
			%s, -- end_snapshot
			%s, -- file_order
			'%s', -- path
			%s, -- path_is_relative
			'%s', -- file_format
			%d, -- record_count
			%d, -- file_size_bytes
			%s, -- footer_size
			%s, -- row_id_start
			%d, -- partition_id
			%s, -- encryption_key
			%s, -- mapping_id
			%s -- partial_max
		);
	)";

	return StringUtil::Format(DATA_FILE_SQL,
	                          // data_file_id
	                          data_file_id,
	                          // table_id
	                          table_id,
	                          // begin_snapshot
	                          snapshot_ids.first,
	                          // end_snapshot
	                          snapshot_ids.second,
	                          // file_order
	                          "NULL",
	                          // path
	                          path,
	                          // path_is_relative
	                          "False",
	                          // file_format
	                          "parquet",
	                          // record_count
	                          record_count,
	                          // file_size_bytes
	                          file_size_bytes,
	                          // footer_size
	                          "NULL",
	                          // row_id_start
	                          "NULL",
	                          // partition_id
	                          partition_id,
	                          // encryption_key
	                          "NULL",
	                          // mapping_id
	                          "NULL",
	                          // partial_max
	                          "NULL");
}

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
