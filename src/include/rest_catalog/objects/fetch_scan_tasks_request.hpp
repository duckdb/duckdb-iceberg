
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/plan_task.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class FetchScanTasksRequest {
public:
	FetchScanTasksRequest();
	FetchScanTasksRequest(const FetchScanTasksRequest &) = delete;
	FetchScanTasksRequest &operator=(const FetchScanTasksRequest &) = delete;
	FetchScanTasksRequest(FetchScanTasksRequest &&) = default;
	FetchScanTasksRequest &operator=(FetchScanTasksRequest &&) = default;

public:
	static FetchScanTasksRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	PlanTask plan_task;
};

} // namespace rest_api_objects
} // namespace duckdb
