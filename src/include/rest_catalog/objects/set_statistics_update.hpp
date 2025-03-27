#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/statistics_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SetStatisticsUpdate {
public:
	static SetStatisticsUpdate FromJSON(yyjson_val *obj) {
		SetStatisticsUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		}
		auto statistics_val = yyjson_obj_get(obj, "statistics");
		if (statistics_val) {
			result.statistics = StatisticsFile::FromJSON(statistics_val);
		}
		return result;
	}
public:
	string action;
	int64_t snapshot_id;
	StatisticsFile statistics;
};

} // namespace rest_api_objects
} // namespace duckdb