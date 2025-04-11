
#include "rest_catalog/objects/fetch_scan_tasks_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FetchScanTasksRequest::FetchScanTasksRequest() {
}

FetchScanTasksRequest FetchScanTasksRequest::FromJSON(yyjson_val *obj) {
	FetchScanTasksRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string FetchScanTasksRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto plan_task_val = yyjson_obj_get(obj, "plan-task");
	if (!plan_task_val) {
		return "FetchScanTasksRequest required property 'plan-task' is missing";
	} else {
		error = plan_task.TryFromJSON(plan_task_val);
		if (!error.empty()) {
			return error;
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
