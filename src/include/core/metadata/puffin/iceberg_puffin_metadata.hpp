#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include <variant>

namespace duckdb {

struct IcebergPuffinBlobMetadata {
public:
	string type;
	vector<int32_t> fields;
	int64_t snapshot_id = 0;
	int64_t sequence_number = 0;
	int64_t offset = 0;
	int64_t length = 0;
	optional<string> compression_codec;
	optional<case_insensitive_map_t<string>> properties;
};

struct IcebergPuffinFileMetadata {
public:
	vector<IcebergPuffinBlobMetadata> blobs;
	optional<case_insensitive_map_t<string>> properties;
};

struct IcebergPuffinFileFooter {
public:
	IcebergPuffinFileMetadata file_metadata;
	idx_t footer_payload_size = 0;
	idx_t footer_size = 0;
};

using IcebergPuffinFileFooterResult = std::variant<IcebergPuffinFileFooter, string>;

//! Tracks the exact size of an uncompressed Puffin file, including its JSON footer.
class IcebergPuffinFileLayout {
public:
	IcebergPuffinFileLayout();
	//! Account for a blob and return the resulting file size.
	idx_t AddBlob(idx_t size, IcebergPuffinBlobMetadata metadata);

private:
	idx_t data_size;
	idx_t footer_size;
	idx_t blob_count = 0;
};

//! Streams blobs to one Puffin file and records their individual byte ranges in its footer.
class IcebergPuffinWriter {
public:
	IcebergPuffinWriter(FileSystem &fs, const string &path);
	idx_t WriteBlob(const vector<data_t> &blob, IcebergPuffinBlobMetadata metadata);
	//! Write the footer, close the file, and return the shared file size.
	idx_t Finish();
	const string &GetPath() const {
		return writer.path;
	}

private:
	BufferedFileWriter writer;
	vector<IcebergPuffinBlobMetadata> blobs;
};

class IcebergPuffinReader {
public:
	static IcebergPuffinFileFooterResult ReadFooter(FileSystem &fs, FileHandle &handle, const string &path,
	                                                optional<int64_t> expected_file_size = optional<int64_t>(),
	                                                optional<int64_t> expected_footer_size = optional<int64_t>());
	static IcebergPuffinFileFooterResult ReadFooter(FileSystem &fs, const string &path,
	                                                optional<int64_t> expected_file_size = optional<int64_t>(),
	                                                optional<int64_t> expected_footer_size = optional<int64_t>());
};

} // namespace duckdb
