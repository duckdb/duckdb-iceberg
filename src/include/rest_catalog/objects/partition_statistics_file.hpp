
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PartitionStatisticsFile {
public:
	PartitionStatisticsFile();
	PartitionStatisticsFile(const PartitionStatisticsFile &) = delete;
	PartitionStatisticsFile &operator=(const PartitionStatisticsFile &) = delete;
	PartitionStatisticsFile(PartitionStatisticsFile &&) = default;
	PartitionStatisticsFile &operator=(PartitionStatisticsFile &&) = default;

public:
	static PartitionStatisticsFile FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int64_t snapshot_id;
	string statistics_path;
	int64_t file_size_in_bytes;
};

} // namespace rest_api_objects
} // namespace duckdb
