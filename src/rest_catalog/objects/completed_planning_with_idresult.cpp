
#include "rest_catalog/objects/completed_planning_with_idresult.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CompletedPlanningWithIDResult::CompletedPlanningWithIDResult() {
}
CompletedPlanningWithIDResult::Object6::Object6() {
}

CompletedPlanningWithIDResult::Object6 CompletedPlanningWithIDResult::Object6::FromJSON(yyjson_val *obj) {
	Object6 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string CompletedPlanningWithIDResult::Object6::TryFromJSON(yyjson_val *obj) {
	string error;
	auto plan_id_val = yyjson_obj_get(obj, "plan-id");
	if (plan_id_val) {
		has_plan_id = true;
		if (yyjson_is_str(plan_id_val)) {
			plan_id = yyjson_get_str(plan_id_val);
		} else {
			return StringUtil::Format("Object6 property 'plan_id' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(plan_id_val));
		}
	}
	return string();
}

CompletedPlanningWithIDResult CompletedPlanningWithIDResult::FromJSON(yyjson_val *obj) {
	CompletedPlanningWithIDResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string CompletedPlanningWithIDResult::TryFromJSON(yyjson_val *obj) {
	string error;
	error = completed_planning_result.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = object_6.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
