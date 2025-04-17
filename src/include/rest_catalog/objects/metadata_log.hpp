
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class MetadataLog {
public:
	MetadataLog();
	MetadataLog(const MetadataLog &) = delete;
	MetadataLog &operator=(const MetadataLog &) = delete;
	MetadataLog(MetadataLog &&) = default;
	MetadataLog &operator=(MetadataLog &&) = default;
	class Object4 {
	public:
		Object4();
		Object4(const Object4 &) = delete;
		Object4 &operator=(const Object4 &) = delete;
		Object4(Object4 &&) = default;
		Object4 &operator=(Object4 &&) = default;

	public:
		static Object4 FromJSON(yyjson_val *obj);

	public:
		string TryFromJSON(yyjson_val *obj);

	public:
		string metadata_file;
		int64_t timestamp_ms;
	};

public:
	static MetadataLog FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<Object4> value;
};

} // namespace rest_api_objects
} // namespace duckdb
