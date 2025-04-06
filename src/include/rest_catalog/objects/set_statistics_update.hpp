
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
	SetStatisticsUpdate() {
	}

public:
	static SetStatisticsUpdate FromJSON(yyjson_val *obj) {
		SetStatisticsUpdate res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		error = base_update.TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
		auto statistics_val = yyjson_obj_get(obj, "statistics");
		if (!statistics_val) {
			return "SetStatisticsUpdate required property 'statistics' is missing";
		} else {
			error = statistics.TryFromJSON(statistics_val);
			if (!error.empty()) {
				return error;
			}
		}
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			action = yyjson_get_str(action_val);
		}
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot_id");
		if (snapshot_id_val) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		}
		return string();
	}

public:
	BaseUpdate base_update;
	StatisticsFile statistics;
	string action;
	int64_t snapshot_id;
};

} // namespace rest_api_objects
} // namespace duckdb
