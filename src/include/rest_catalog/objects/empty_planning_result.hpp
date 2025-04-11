
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/plan_status.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class EmptyPlanningResult {
public:
	EmptyPlanningResult();
	EmptyPlanningResult(const EmptyPlanningResult &) = delete;
	EmptyPlanningResult &operator=(const EmptyPlanningResult &) = delete;
	EmptyPlanningResult(EmptyPlanningResult &&) = default;
	EmptyPlanningResult &operator=(EmptyPlanningResult &&) = default;

public:
	static EmptyPlanningResult FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	PlanStatus status;
};

} // namespace rest_api_objects
} // namespace duckdb
