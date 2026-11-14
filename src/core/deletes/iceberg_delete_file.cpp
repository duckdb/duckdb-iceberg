#include "core/deletes/iceberg_delete_file.hpp"

#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {

// Gets the referenced data files. Only positional deletes reference data files.
optional<string> IcebergDeleteFile::ReferencedDataFile() const {
	if (content != IcebergManifestEntryContentType::POSITION_DELETES) {
		return nullopt;
	}
	if (referenced_data_file) {
		return referenced_data_file;
	}
	// Equal file_path bounds mean every row references the same data file
	auto lower = lower_bounds.find(MultiFileReader::DELETE_FILE_PATH_FIELD_ID);
	auto upper = upper_bounds.find(MultiFileReader::DELETE_FILE_PATH_FIELD_ID);
	if (lower == lower_bounds.end() || upper == upper_bounds.end() || lower->second.IsNull() ||
	    upper->second.IsNull() || lower->second != upper->second) {
		return nullopt;
	}
	return StringValue::Get(lower->second);
}

} // namespace duckdb
