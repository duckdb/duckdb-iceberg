
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/completed_planning_result.hpp"
#include "rest_catalog/objects/empty_planning_result.hpp"
#include "rest_catalog/objects/failed_planning_result.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FetchPlanningResult {
public:
	FetchPlanningResult();
	FetchPlanningResult(const FetchPlanningResult &) = delete;
	FetchPlanningResult &operator=(const FetchPlanningResult &) = delete;
	FetchPlanningResult(FetchPlanningResult &&) = default;
	FetchPlanningResult &operator=(FetchPlanningResult &&) = default;

public:
	static FetchPlanningResult FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	CompletedPlanningResult completed_planning_result;
	bool has_completed_planning_result = false;
	FailedPlanningResult failed_planning_result;
	bool has_failed_planning_result = false;
	EmptyPlanningResult empty_planning_result;
	bool has_empty_planning_result = false;
};

} // namespace rest_api_objects
} // namespace duckdb
