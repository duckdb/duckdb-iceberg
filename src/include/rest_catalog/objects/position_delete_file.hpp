
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/content_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PositionDeleteFile {
public:
	PositionDeleteFile();
	PositionDeleteFile(const PositionDeleteFile &) = delete;
	PositionDeleteFile &operator=(const PositionDeleteFile &) = delete;
	PositionDeleteFile(PositionDeleteFile &&) = default;
	PositionDeleteFile &operator=(PositionDeleteFile &&) = default;

public:
	static PositionDeleteFile FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	ContentFile content_file;
	string content;
	int64_t content_offset;
	bool has_content_offset = false;
	int64_t content_size_in_bytes;
	bool has_content_size_in_bytes = false;
};

} // namespace rest_api_objects
} // namespace duckdb
