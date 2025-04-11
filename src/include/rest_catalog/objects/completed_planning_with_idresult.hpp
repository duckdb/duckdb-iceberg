
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/completed_planning_result.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CompletedPlanningWithIDResult {
public:
	CompletedPlanningWithIDResult();
	CompletedPlanningWithIDResult(const CompletedPlanningWithIDResult &) = delete;
	CompletedPlanningWithIDResult &operator=(const CompletedPlanningWithIDResult &) = delete;
	CompletedPlanningWithIDResult(CompletedPlanningWithIDResult &&) = default;
	CompletedPlanningWithIDResult &operator=(CompletedPlanningWithIDResult &&) = default;
	class Object6 {
	public:
		Object6();
		Object6(const Object6 &) = delete;
		Object6 &operator=(const Object6 &) = delete;
		Object6(Object6 &&) = default;
		Object6 &operator=(Object6 &&) = default;

	public:
		static Object6 FromJSON(yyjson_val *obj);

	public:
		string TryFromJSON(yyjson_val *obj);

	public:
		string plan_id;
		bool has_plan_id = false;
	};

public:
	static CompletedPlanningWithIDResult FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	CompletedPlanningResult completed_planning_result;
	Object6 object_6;
};

} // namespace rest_api_objects
} // namespace duckdb
