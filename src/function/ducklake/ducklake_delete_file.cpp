#include "function/ducklake/ducklake_delete_file.hpp"
#include "function/ducklake/ducklake_utils.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

DuckLakeDeleteFile::DuckLakeDeleteFile(const IcebergManifestEntry &manifest_entry, const string &table_name) {
	auto &data_file = manifest_entry.data_file;
	path = data_file.file_path;
	if (!StringUtil::CIEquals(data_file.file_format, "parquet")) {
		throw InvalidInputException("Can't convert Iceberg table (name: %s) to DuckLake, as it contains a delete "
		                            "files with file_format '%s'",
		                            table_name, data_file.file_format);
	}
	record_count = data_file.record_count;
	file_size_bytes = data_file.file_size_in_bytes;

	//! Find lower and upper bounds for the 'file_path' of the position delete file
	auto lower_bound_it = data_file.lower_bounds.find(2147483546);
	auto upper_bound_it = data_file.upper_bounds.find(2147483546);
	if (lower_bound_it == data_file.lower_bounds.end() || upper_bound_it == data_file.upper_bounds.end()) {
		throw InvalidInputException(
		    "No lower/upper bounds are available for the Position Delete File for table %s, this is "
		    "required for export to DuckLake",
		    table_name);
	}

	auto &lower_bound = lower_bound_it->second;
	auto &upper_bound = upper_bound_it->second;

	if (lower_bound.IsNull() || upper_bound.IsNull()) {
		throw InvalidInputException("Lower and Upper bounds for a Position Delete File can not be NULL!");
	}

	if (lower_bound != upper_bound) {
		throw InvalidInputException("For a Position Delete File to be eligible for conversion to DuckLake, it can "
		                            "only reference a single data file");
	}
	data_file_path = lower_bound.GetValue<string>();
}

string DuckLakeDeleteFile::FinalizeEntry(int64_t table_id, vector<DuckLakeDataFile> &all_data_files,
                                         const map<timestamp_t, DuckLakeSnapshot> &snapshots) {
	auto &snapshot = snapshots.at(start_snapshot);
	int64_t delete_file_id = snapshot.base_file_id + file_id_offset;
	this->delete_file_id = delete_file_id;

	auto snapshot_ids = DuckLakeUtils::GetSnapshots(start_snapshot, has_end, end_snapshot, snapshots);

	auto &data_file = all_data_files[referenced_data_file];
	int64_t data_file_id = data_file.data_file_id.GetIndex();

	return StringUtil::Format("VALUES(%d, %d, %d, %s, %d, '%s', false, 'parquet', %d, %d, NULL, NULL);", delete_file_id,
	                          table_id, snapshot_ids.first, snapshot_ids.second, data_file_id, path, record_count,
	                          file_size_bytes);
}

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
