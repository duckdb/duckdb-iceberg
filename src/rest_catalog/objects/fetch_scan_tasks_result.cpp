
#include "rest_catalog/objects/fetch_scan_tasks_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FetchScanTasksResult::FetchScanTasksResult() {
}

FetchScanTasksResult FetchScanTasksResult::FromJSON(yyjson_val *obj) {
	FetchScanTasksResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string FetchScanTasksResult::TryFromJSON(yyjson_val *obj) {
	string error;
	error = scan_tasks.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
