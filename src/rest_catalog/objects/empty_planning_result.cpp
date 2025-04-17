
#include "rest_catalog/objects/empty_planning_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

EmptyPlanningResult::EmptyPlanningResult() {
}

EmptyPlanningResult EmptyPlanningResult::FromJSON(yyjson_val *obj) {
	EmptyPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string EmptyPlanningResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto status_val = yyjson_obj_get(obj, "status");
	if (!status_val) {
		return "EmptyPlanningResult required property 'status' is missing";
	} else {
		error = status.TryFromJSON(status_val);
		if (!error.empty()) {
			return error;
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
