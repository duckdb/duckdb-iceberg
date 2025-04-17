
#include "rest_catalog/objects/failed_planning_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FailedPlanningResult::FailedPlanningResult() {
}
FailedPlanningResult::Object7::Object7() {
}

FailedPlanningResult::Object7 FailedPlanningResult::Object7::FromJSON(yyjson_val *obj) {
	Object7 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string FailedPlanningResult::Object7::TryFromJSON(yyjson_val *obj) {
	string error;
	auto status_val = yyjson_obj_get(obj, "status");
	if (!status_val) {
		return "Object7 required property 'status' is missing";
	} else {
		error = status.TryFromJSON(status_val);
		if (!error.empty()) {
			return error;
		}
	}
	return string();
}

FailedPlanningResult FailedPlanningResult::FromJSON(yyjson_val *obj) {
	FailedPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string FailedPlanningResult::TryFromJSON(yyjson_val *obj) {
	string error;
	error = iceberg_error_response.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = object_7.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
