
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/plan_task.hpp"

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
	// Deserialization
	static FetchScanTasksRequest FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FetchScanTasksRequest Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	PlanTask plan_task;
};

} // namespace rest_api_objects
} // namespace duckdb
