
#include "rest_catalog/objects/async_planning_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AsyncPlanningResult::AsyncPlanningResult() {
}

AsyncPlanningResult AsyncPlanningResult::FromJSON(yyjson_val *obj) {
	AsyncPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string AsyncPlanningResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto status_val = yyjson_obj_get(obj, "status");
	if (!status_val) {
		return "AsyncPlanningResult required property 'status' is missing";
	} else {
		error = status.TryFromJSON(status_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto plan_id_val = yyjson_obj_get(obj, "plan-id");
	if (plan_id_val) {
		has_plan_id = true;
		plan_id = yyjson_get_str(plan_id_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
