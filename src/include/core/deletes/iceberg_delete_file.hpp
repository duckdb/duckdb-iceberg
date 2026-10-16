#pragma once

#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

//! Execution descriptor: all information needed to read a selected delete file.
//! Applicability and sequence inheritance have already been resolved by planning.
struct IcebergDeleteFile {
	IcebergDeleteFile() = default;
	explicit IcebergDeleteFile(const IcebergDataFile &file)
	    : file_path(file.file_path), file_format(file.file_format), content(file.content),
	      file_size_in_bytes(file.file_size_in_bytes), record_count(file.record_count), equality_ids(file.equality_ids),
	      referenced_data_file(file.referenced_data_file), content_offset(file.content_offset),
	      content_size_in_bytes(file.content_size_in_bytes) {
	}

	bool operator==(const IcebergDeleteFile &other) const {
		return file_path == other.file_path && file_format == other.file_format && content == other.content &&
		       file_size_in_bytes == other.file_size_in_bytes && record_count == other.record_count &&
		       equality_ids == other.equality_ids && referenced_data_file == other.referenced_data_file &&
		       content_offset == other.content_offset && content_size_in_bytes == other.content_size_in_bytes;
	}

	string file_path;
	string file_format;
	IcebergManifestEntryContentType content = IcebergManifestEntryContentType::POSITION_DELETES;
	int64_t file_size_in_bytes = 0;
	int64_t record_count = 0;
	vector<int32_t> equality_ids;
	optional<string> referenced_data_file;
	optional<int64_t> content_offset;
	optional<int64_t> content_size_in_bytes;
};

} // namespace duckdb
