
#include "rest_catalog/objects/fetch_scan_tasks_request.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FetchScanTasksRequest::FetchScanTasksRequest() {
}

FetchScanTasksRequest FetchScanTasksRequest::FromJSON(JSONValue obj) {
	FetchScanTasksRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FetchScanTasksRequest FetchScanTasksRequest::Copy() const {
	FetchScanTasksRequest res;
	res.plan_task = plan_task.Copy();
	return res;
}

string FetchScanTasksRequest::TryFromJSON(JSONValue obj) {
	string error;
	auto plan_task_val = obj.GetMember("plan-task");
	if (!plan_task_val.IsValid()) {
		return "FetchScanTasksRequest required property 'plan-task' is missing";
	} else {
		error = plan_task.TryFromJSON(plan_task_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void FetchScanTasksRequest::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: plan-task
	auto plan_task_json = plan_task.ToJSON(writer);
	obj.Add("plan-task", plan_task_json);
}

JSONMutableValue FetchScanTasksRequest::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
