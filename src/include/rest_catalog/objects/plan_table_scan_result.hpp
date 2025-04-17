
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/async_planning_result.hpp"
#include "rest_catalog/objects/completed_planning_with_idresult.hpp"
#include "rest_catalog/objects/empty_planning_result.hpp"
#include "rest_catalog/objects/failed_planning_result.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PlanTableScanResult {
public:
	PlanTableScanResult();
	PlanTableScanResult(const PlanTableScanResult &) = delete;
	PlanTableScanResult &operator=(const PlanTableScanResult &) = delete;
	PlanTableScanResult(PlanTableScanResult &&) = default;
	PlanTableScanResult &operator=(PlanTableScanResult &&) = default;

public:
	static PlanTableScanResult FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	CompletedPlanningWithIDResult completed_planning_with_idresult;
	bool has_completed_planning_with_idresult = false;
	FailedPlanningResult failed_planning_result;
	bool has_failed_planning_result = false;
	AsyncPlanningResult async_planning_result;
	bool has_async_planning_result = false;
	EmptyPlanningResult empty_planning_result;
	bool has_empty_planning_result = false;
};

} // namespace rest_api_objects
} // namespace duckdb
