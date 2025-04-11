
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/plan_status.hpp"
#include "rest_catalog/objects/scan_tasks.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CompletedPlanningResult {
public:
	CompletedPlanningResult();
	CompletedPlanningResult(const CompletedPlanningResult &) = delete;
	CompletedPlanningResult &operator=(const CompletedPlanningResult &) = delete;
	CompletedPlanningResult(CompletedPlanningResult &&) = default;
	CompletedPlanningResult &operator=(CompletedPlanningResult &&) = default;
	class Object5 {
	public:
		Object5();
		Object5(const Object5 &) = delete;
		Object5 &operator=(const Object5 &) = delete;
		Object5(Object5 &&) = default;
		Object5 &operator=(Object5 &&) = default;

	public:
		static Object5 FromJSON(yyjson_val *obj);

	public:
		string TryFromJSON(yyjson_val *obj);

	public:
		PlanStatus status;
	};

public:
	static CompletedPlanningResult FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	ScanTasks scan_tasks;
	Object5 object_5;
};

} // namespace rest_api_objects
} // namespace duckdb
