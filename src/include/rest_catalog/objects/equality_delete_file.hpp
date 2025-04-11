
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

class EqualityDeleteFile {
public:
	EqualityDeleteFile();
	EqualityDeleteFile(const EqualityDeleteFile &) = delete;
	EqualityDeleteFile &operator=(const EqualityDeleteFile &) = delete;
	EqualityDeleteFile(EqualityDeleteFile &&) = default;
	EqualityDeleteFile &operator=(EqualityDeleteFile &&) = default;

public:
	static EqualityDeleteFile FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	ContentFile content_file;
	string content;
	vector<int64_t> equality_ids;
	bool has_equality_ids = false;
};

} // namespace rest_api_objects
} // namespace duckdb
