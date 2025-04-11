
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/statistics_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetStatisticsUpdate {
public:
	SetStatisticsUpdate();
	SetStatisticsUpdate(const SetStatisticsUpdate &) = delete;
	SetStatisticsUpdate &operator=(const SetStatisticsUpdate &) = delete;
	SetStatisticsUpdate(SetStatisticsUpdate &&) = default;
	SetStatisticsUpdate &operator=(SetStatisticsUpdate &&) = default;

public:
	static SetStatisticsUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	StatisticsFile statistics;
	string action;
	bool has_action = false;
	int64_t snapshot_id;
	bool has_snapshot_id = false;
};

} // namespace rest_api_objects
} // namespace duckdb
