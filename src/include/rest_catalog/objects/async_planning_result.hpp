
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/plan_status.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AsyncPlanningResult {
public:
	AsyncPlanningResult();
	AsyncPlanningResult(const AsyncPlanningResult &) = delete;
	AsyncPlanningResult &operator=(const AsyncPlanningResult &) = delete;
	AsyncPlanningResult(AsyncPlanningResult &&) = default;
	AsyncPlanningResult &operator=(AsyncPlanningResult &&) = default;

public:
	static AsyncPlanningResult FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	PlanStatus status;
	string plan_id;
};

} // namespace rest_api_objects
} // namespace duckdb
