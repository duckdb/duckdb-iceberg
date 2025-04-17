
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class RemovePartitionStatisticsUpdate {
public:
	RemovePartitionStatisticsUpdate();
	RemovePartitionStatisticsUpdate(const RemovePartitionStatisticsUpdate &) = delete;
	RemovePartitionStatisticsUpdate &operator=(const RemovePartitionStatisticsUpdate &) = delete;
	RemovePartitionStatisticsUpdate(RemovePartitionStatisticsUpdate &&) = default;
	RemovePartitionStatisticsUpdate &operator=(RemovePartitionStatisticsUpdate &&) = default;

public:
	static RemovePartitionStatisticsUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	int64_t snapshot_id;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
