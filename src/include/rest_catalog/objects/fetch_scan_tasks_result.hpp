
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/scan_tasks.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FetchScanTasksResult {
public:
	FetchScanTasksResult();
	FetchScanTasksResult(const FetchScanTasksResult &) = delete;
	FetchScanTasksResult &operator=(const FetchScanTasksResult &) = delete;
	FetchScanTasksResult(FetchScanTasksResult &&) = default;
	FetchScanTasksResult &operator=(FetchScanTasksResult &&) = default;

public:
	static FetchScanTasksResult FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	ScanTasks scan_tasks;
};

} // namespace rest_api_objects
} // namespace duckdb
