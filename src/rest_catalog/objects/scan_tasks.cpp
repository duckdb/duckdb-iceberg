
#include "rest_catalog/objects/scan_tasks.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

ScanTasks::ScanTasks() {
}

ScanTasks ScanTasks::FromJSON(JSONValue obj) {
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

string ScanTasks::TryFromJSON(JSONValue obj) {
	string error;
	auto delete_files_val = obj.GetMember("delete-files");
	if (delete_files_val.IsValid()) {
		vector<DeleteFile> delete_files_tmp;
		if (delete_files_val.IsArray()) {
			delete_files_val.IterateArray([&](JSONValue delete_files_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				DeleteFile delete_files_tmp_item;
				error = delete_files_tmp_item.TryFromJSON(delete_files_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				delete_files_tmp.emplace_back(std::move(delete_files_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ScanTasks property 'delete_files_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(delete_files_val).c_str());
		}
		delete_files = std::move(delete_files_tmp);
	}
	auto file_scan_tasks_val = obj.GetMember("file-scan-tasks");
	if (file_scan_tasks_val.IsValid()) {
		vector<FileScanTask> file_scan_tasks_tmp;
		if (file_scan_tasks_val.IsArray()) {
			file_scan_tasks_val.IterateArray([&](JSONValue file_scan_tasks_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				FileScanTask file_scan_tasks_tmp_item;
				error = file_scan_tasks_tmp_item.TryFromJSON(file_scan_tasks_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				file_scan_tasks_tmp.emplace_back(std::move(file_scan_tasks_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "ScanTasks property 'file_scan_tasks_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(file_scan_tasks_val).c_str());
		}
		file_scan_tasks = std::move(file_scan_tasks_tmp);
	}
	auto plan_tasks_val = obj.GetMember("plan-tasks");
	if (plan_tasks_val.IsValid()) {
		vector<PlanTask> plan_tasks_tmp;
		if (plan_tasks_val.IsArray()) {
			plan_tasks_val.IterateArray([&](JSONValue plan_tasks_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				PlanTask plan_tasks_tmp_item;
				error = plan_tasks_tmp_item.TryFromJSON(plan_tasks_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				plan_tasks_tmp.emplace_back(std::move(plan_tasks_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("ScanTasks property 'plan_tasks_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(plan_tasks_val).c_str());
		}
		plan_tasks = std::move(plan_tasks_tmp);
	}
	return "";
}

void ScanTasks::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: delete-files
	if (delete_files.has_value()) {
		auto &delete_files_value = *delete_files;
		auto delete_files_json = writer.CreateArray();
		for (const auto &delete_files_json_item : delete_files_value) {
			auto delete_files_json_item_json = delete_files_json_item.ToJSON(writer);
			delete_files_json.Append(delete_files_json_item_json);
		}
		obj.Add("delete-files", delete_files_json);
	}

	// Serialize: file-scan-tasks
	if (file_scan_tasks.has_value()) {
		auto &file_scan_tasks_value = *file_scan_tasks;
		auto file_scan_tasks_json = writer.CreateArray();
		for (const auto &file_scan_tasks_json_item : file_scan_tasks_value) {
			auto file_scan_tasks_json_item_json = file_scan_tasks_json_item.ToJSON(writer);
			file_scan_tasks_json.Append(file_scan_tasks_json_item_json);
		}
		obj.Add("file-scan-tasks", file_scan_tasks_json);
	}

	// Serialize: plan-tasks
	if (plan_tasks.has_value()) {
		auto &plan_tasks_value = *plan_tasks;
		auto plan_tasks_json = writer.CreateArray();
		for (const auto &plan_tasks_json_item : plan_tasks_value) {
			auto plan_tasks_json_item_json = plan_tasks_json_item.ToJSON(writer);
			plan_tasks_json.Append(plan_tasks_json_item_json);
		}
		obj.Add("plan-tasks", plan_tasks_json);
	}
}

JSONMutableValue ScanTasks::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
