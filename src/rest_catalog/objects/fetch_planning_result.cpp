
#include "rest_catalog/objects/fetch_planning_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FetchPlanningResult::FetchPlanningResult() {
}

FetchPlanningResult FetchPlanningResult::FromJSON(yyjson_val *obj) {
	FetchPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string FetchPlanningResult::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = completed_planning_result.TryFromJSON(obj);
		if (error.empty()) {
			has_completed_planning_result = true;
			break;
		}
		error = failed_planning_result.TryFromJSON(obj);
		if (error.empty()) {
			has_failed_planning_result = true;
			break;
		}
		error = empty_planning_result.TryFromJSON(obj);
		if (error.empty()) {
			has_empty_planning_result = true;
			break;
		}
		return "FetchPlanningResult failed to parse, none of the oneOf candidates matched";
	} while (false);
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
