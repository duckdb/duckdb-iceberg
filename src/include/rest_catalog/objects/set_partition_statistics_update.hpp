
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/partition_statistics_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetPartitionStatisticsUpdate {
public:
	SetPartitionStatisticsUpdate();
	SetPartitionStatisticsUpdate(const SetPartitionStatisticsUpdate &) = delete;
	SetPartitionStatisticsUpdate &operator=(const SetPartitionStatisticsUpdate &) = delete;
	SetPartitionStatisticsUpdate(SetPartitionStatisticsUpdate &&) = default;
	SetPartitionStatisticsUpdate &operator=(SetPartitionStatisticsUpdate &&) = default;

public:
	static SetPartitionStatisticsUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	PartitionStatisticsFile partition_statistics;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
