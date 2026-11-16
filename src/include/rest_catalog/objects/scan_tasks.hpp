
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/delete_file.hpp"
#include "rest_catalog/objects/file_scan_task.hpp"
#include "rest_catalog/objects/plan_task.hpp"

namespace duckdb {
namespace rest_api_objects {

class ScanTasks {
public:
	ScanTasks();
	ScanTasks(const ScanTasks &) = delete;
	ScanTasks &operator=(const ScanTasks &) = delete;
	ScanTasks(ScanTasks &&) = default;
	ScanTasks &operator=(ScanTasks &&) = default;

public:
	// Deserialization
	static ScanTasks FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ScanTasks Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<vector<DeleteFile>> delete_files;
	optional<vector<FileScanTask>> file_scan_tasks;
	optional<vector<PlanTask>> plan_tasks;
};

} // namespace rest_api_objects
} // namespace duckdb
