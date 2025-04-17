
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/blob_metadata.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class StatisticsFile {
public:
	StatisticsFile();
	StatisticsFile(const StatisticsFile &) = delete;
	StatisticsFile &operator=(const StatisticsFile &) = delete;
	StatisticsFile(StatisticsFile &&) = default;
	StatisticsFile &operator=(StatisticsFile &&) = default;

public:
	static StatisticsFile FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int64_t snapshot_id;
	string statistics_path;
	int64_t file_size_in_bytes;
	int64_t file_footer_size_in_bytes;
	vector<BlobMetadata> blob_metadata;
};

} // namespace rest_api_objects
} // namespace duckdb
