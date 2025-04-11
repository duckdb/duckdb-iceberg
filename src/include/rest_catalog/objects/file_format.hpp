
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FileFormat {
public:
	FileFormat();
	FileFormat(const FileFormat &) = delete;
	FileFormat &operator=(const FileFormat &) = delete;
	FileFormat(FileFormat &&) = default;
	FileFormat &operator=(FileFormat &&) = default;

public:
	static FileFormat FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
