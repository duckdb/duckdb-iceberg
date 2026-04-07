#include "function/ducklake/ducklake_delete_file.hpp"
#include "function/ducklake/ducklake_utils.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

DuckLakeDeleteFile::DuckLakeDeleteFile(const IcebergManifestEntry &manifest_entry, const string &table_name) {
	auto &data_file = manifest_entry.data_file;
	path = data_file.file_path;
	record_count = data_file.record_count;
	file_size_bytes = data_file.file_size_in_bytes;

	file_format = StringUtil::Lower(data_file.file_format);
	if (file_format == "parquet") {
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
	} else if (file_format == "puffin") {
		auto content_offset = data_file.content_offset.GetValue<int64_t>();
		auto content_size_in_bytes = data_file.content_size_in_bytes.GetValue<int64_t>();
		if (content_offset != 0) {
			throw InvalidInputException(
			    "Only deletion vectors that start at offset 0 can be converted to DuckLake currently");
		}
		if (content_size_in_bytes != data_file.file_size_in_bytes) {
			throw InvalidInputException("Only deletion vectors that have 'content_size_in_bytes' equal to "
			                            "'file_size_in_bytes' can be converted to DuckLake currently");
		}
		data_file_path = data_file.referenced_data_file;
	} else {
		throw InvalidInputException("Can't convert Iceberg table (name: %s) to DuckLake, as it contains a delete "
		                            "files with file_format '%s'",
		                            table_name, file_format);
	}
}

string DuckLakeDeleteFile::FinalizeEntry(int64_t table_id, vector<DuckLakeDataFile> &all_data_files,
                                         const map<timestamp_t, DuckLakeSnapshot> &snapshots) {
	auto &snapshot = snapshots.at(start_snapshot);
	int64_t delete_file_id = snapshot.base_file_id + file_id_offset;
	this->delete_file_id = delete_file_id;

	auto snapshot_ids = DuckLakeUtils::GetSnapshots(start_snapshot, has_end, end_snapshot, snapshots);

	auto &data_file = all_data_files[referenced_data_file];
	int64_t data_file_id = data_file.data_file_id.GetIndex();

	const auto DELETE_FILE_SQL = R"(
		INSERT INTO {METADATA_CATALOG}.ducklake_delete_file VALUES (
			%d, -- delete_file_id
			%d, -- table_id
			%d, -- begin_snapshot
			%s, -- end_snapshot
			%d, -- data_file_id
			'%s', -- path
			%s, -- path_is_relative
			'%s', -- format
			%d, -- delete_count
			%d, -- file_size_bytes
			%s, -- footer_size
			%s, -- encryption_key
			%s -- partial_max
		);
	)";

	return StringUtil::Format(DELETE_FILE_SQL,
	                          // delete_file_id
	                          delete_file_id,
	                          // table_id
	                          table_id,
	                          // begin_snapshot
	                          snapshot_ids.first,
	                          // end_snapshot
	                          snapshot_ids.second,
	                          // data_file_id
	                          data_file_id,
	                          // path
	                          path,
	                          // path_is_relative
	                          "false",
	                          // format
	                          file_format,
	                          // delete_count
	                          record_count,
	                          // file_size_bytes
	                          file_size_bytes,
	                          // footer_size
	                          "NULL",
	                          // encryption_key
	                          "NULL",
	                          // partial_max
	                          "NULL");
}

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
