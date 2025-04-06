
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
	SetPartitionStatisticsUpdate() {
	}

public:
	static SetPartitionStatisticsUpdate FromJSON(yyjson_val *obj) {
		SetPartitionStatisticsUpdate res;
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

		auto partition_statistics_val = yyjson_obj_get(obj, "partition_statistics");
		if (!partition_statistics_val) {
			return "SetPartitionStatisticsUpdate required property 'partition_statistics' is missing";
		} else {
			error = partition_statistics.TryFromJSON(partition_statistics_val);
			if (!error.empty()) {
				return error;
			}
		}

		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			action = yyjson_get_str(action_val);
		}

		return string();
	}

public:
	BaseUpdate base_update;

public:
	string action;
	PartitionStatisticsFile partition_statistics;
};

} // namespace rest_api_objects
} // namespace duckdb
