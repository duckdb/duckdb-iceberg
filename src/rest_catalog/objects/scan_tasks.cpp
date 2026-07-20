
#include "rest_catalog/objects/scan_tasks.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

ScanTasks ScanTasks::Copy() const {
	ScanTasks res;
	if (delete_files.has_value()) {
		res.delete_files.emplace();
		(*res.delete_files).reserve((*delete_files).size());
		for (auto &item : (*delete_files)) {
			(*res.delete_files).emplace_back(item.Copy());
		}
	}
	if (file_scan_tasks.has_value()) {
		res.file_scan_tasks.emplace();
		(*res.file_scan_tasks).reserve((*file_scan_tasks).size());
		for (auto &item : (*file_scan_tasks)) {
			(*res.file_scan_tasks).emplace_back(item.Copy());
		}
	}
	if (plan_tasks.has_value()) {
		res.plan_tasks.emplace();
		(*res.plan_tasks).reserve((*plan_tasks).size());
		for (auto &item : (*plan_tasks)) {
			(*res.plan_tasks).emplace_back(item.Copy());
		}
	}
	return res;
}

string ScanTasks::TryFromJSON(yyjson_val *obj) {
	string error;
	auto delete_files_val = yyjson_obj_get(obj, "delete-files");
	if (delete_files_val) {
		vector<DeleteFile> delete_files_tmp;
		if (yyjson_is_arr(delete_files_val)) {
			size_t delete_files_tmp_idx, delete_files_tmp_max;
			yyjson_val *delete_files_tmp_item_val;
			yyjson_arr_foreach(delete_files_val, delete_files_tmp_idx, delete_files_tmp_max,
			                   delete_files_tmp_item_val) {
				DeleteFile delete_files_tmp_item;
				error = delete_files_tmp_item.TryFromJSON(delete_files_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				delete_files_tmp.emplace_back(std::move(delete_files_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "ScanTasks property 'delete_files_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(delete_files_val));
		}
		delete_files = std::move(delete_files_tmp);
	}
	auto file_scan_tasks_val = yyjson_obj_get(obj, "file-scan-tasks");
	if (file_scan_tasks_val) {
		vector<FileScanTask> file_scan_tasks_tmp;
		if (yyjson_is_arr(file_scan_tasks_val)) {
			size_t file_scan_tasks_tmp_idx, file_scan_tasks_tmp_max;
			yyjson_val *file_scan_tasks_tmp_item_val;
			yyjson_arr_foreach(file_scan_tasks_val, file_scan_tasks_tmp_idx, file_scan_tasks_tmp_max,
			                   file_scan_tasks_tmp_item_val) {
				FileScanTask file_scan_tasks_tmp_item;
				error = file_scan_tasks_tmp_item.TryFromJSON(file_scan_tasks_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				file_scan_tasks_tmp.emplace_back(std::move(file_scan_tasks_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "ScanTasks property 'file_scan_tasks_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(file_scan_tasks_val));
		}
		file_scan_tasks = std::move(file_scan_tasks_tmp);
	}
	auto plan_tasks_val = yyjson_obj_get(obj, "plan-tasks");
	if (plan_tasks_val) {
		vector<PlanTask> plan_tasks_tmp;
		if (yyjson_is_arr(plan_tasks_val)) {
			size_t plan_tasks_tmp_idx, plan_tasks_tmp_max;
			yyjson_val *plan_tasks_tmp_item_val;
			yyjson_arr_foreach(plan_tasks_val, plan_tasks_tmp_idx, plan_tasks_tmp_max, plan_tasks_tmp_item_val) {
				PlanTask plan_tasks_tmp_item;
				error = plan_tasks_tmp_item.TryFromJSON(plan_tasks_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				plan_tasks_tmp.emplace_back(std::move(plan_tasks_tmp_item));
			}
		} else {
			return StringUtil::Format("ScanTasks property 'plan_tasks_tmp' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(plan_tasks_val));
		}
		plan_tasks = std::move(plan_tasks_tmp);
	}
	return "";
}

void ScanTasks::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: delete-files
	if (delete_files.has_value()) {
		auto &delete_files_value = *delete_files;
		yyjson_mut_val *delete_files_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : delete_files_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(delete_files_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "delete-files", delete_files_value_arr);
	}

	// Serialize: file-scan-tasks
	if (file_scan_tasks.has_value()) {
		auto &file_scan_tasks_value = *file_scan_tasks;
		yyjson_mut_val *file_scan_tasks_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : file_scan_tasks_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(file_scan_tasks_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "file-scan-tasks", file_scan_tasks_value_arr);
	}

	// Serialize: plan-tasks
	if (plan_tasks.has_value()) {
		auto &plan_tasks_value = *plan_tasks;
		yyjson_mut_val *plan_tasks_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : plan_tasks_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(plan_tasks_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "plan-tasks", plan_tasks_value_arr);
	}
}

yyjson_mut_val *ScanTasks::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
