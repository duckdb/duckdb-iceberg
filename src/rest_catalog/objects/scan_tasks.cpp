
#include "rest_catalog/objects/scan_tasks.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ScanTasks::ScanTasks() {
}

ScanTasks ScanTasks::FromJSON(yyjson_val *obj) {
	ScanTasks res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ScanTasks::TryFromJSON(yyjson_val *obj) {
	string error;
	auto delete_files_val = yyjson_obj_get(obj, "delete_files");
	if (delete_files_val) {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(delete_files_val, idx, max, val) {
			DeleteFile tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			delete_files.emplace_back(std::move(tmp));
		}
	}
	auto file_scan_tasks_val = yyjson_obj_get(obj, "file_scan_tasks");
	if (file_scan_tasks_val) {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(file_scan_tasks_val, idx, max, val) {
			FileScanTask tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			file_scan_tasks.emplace_back(std::move(tmp));
		}
	}
	auto plan_tasks_val = yyjson_obj_get(obj, "plan_tasks");
	if (plan_tasks_val) {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(plan_tasks_val, idx, max, val) {
			PlanTask tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			plan_tasks.emplace_back(std::move(tmp));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
