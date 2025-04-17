
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/delete_file.hpp"
#include "rest_catalog/objects/file_scan_task.hpp"
#include "rest_catalog/objects/plan_task.hpp"

using namespace duckdb_yyjson;

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
	static ScanTasks FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<DeleteFile> delete_files;
	bool has_delete_files = false;
	vector<FileScanTask> file_scan_tasks;
	bool has_file_scan_tasks = false;
	vector<PlanTask> plan_tasks;
	bool has_plan_tasks = false;
};

} // namespace rest_api_objects
} // namespace duckdb
