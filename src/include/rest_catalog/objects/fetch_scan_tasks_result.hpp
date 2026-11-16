
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/scan_tasks.hpp"

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
	// Deserialization
	static FetchScanTasksResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FetchScanTasksResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	ScanTasks scan_tasks;
};

} // namespace rest_api_objects
} // namespace duckdb
