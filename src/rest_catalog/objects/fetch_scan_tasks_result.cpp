
#include "rest_catalog/objects/fetch_scan_tasks_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FetchScanTasksResult::FetchScanTasksResult() {
}

FetchScanTasksResult FetchScanTasksResult::FromJSON(JSONValue obj) {
	FetchScanTasksResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FetchScanTasksResult FetchScanTasksResult::Copy() const {
	FetchScanTasksResult res;
	res.scan_tasks = scan_tasks.Copy();
	return res;
}

string FetchScanTasksResult::TryFromJSON(JSONValue obj) {
	string error;
	error = scan_tasks.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return "";
}

void FetchScanTasksResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: ScanTasks
	scan_tasks.PopulateJSON(writer, obj);
}

JSONMutableValue FetchScanTasksResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
