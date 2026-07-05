#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

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

class IcebergPuffinReader {
public:
	static IcebergPuffinFileFooter ReadFooter(FileSystem &fs, FileHandle &handle, const string &path,
	                                          optional<int64_t> expected_file_size = optional<int64_t>(),
	                                          optional<int64_t> expected_footer_size = optional<int64_t>());
	static IcebergPuffinFileFooter ReadFooter(FileSystem &fs, const string &path,
	                                          optional<int64_t> expected_file_size = optional<int64_t>(),
	                                          optional<int64_t> expected_footer_size = optional<int64_t>());
};

} // namespace duckdb
