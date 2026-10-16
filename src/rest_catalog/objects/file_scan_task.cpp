
#include "rest_catalog/objects/file_scan_task.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FileScanTask::FileScanTask() {
}

FileScanTask FileScanTask::FromJSON(JSONValue obj) {
	FileScanTask res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FileScanTask FileScanTask::Copy() const {
	FileScanTask res;
	res.data_file = data_file.Copy();
	if (delete_file_references.has_value()) {
		res.delete_file_references.emplace();
		(*res.delete_file_references).reserve((*delete_file_references).size());
		for (auto &item : (*delete_file_references)) {
			(*res.delete_file_references).emplace_back(item);
		}
	}
	if (residual_filter != nullptr) {
		res.residual_filter = residual_filter ? make_uniq<Predicate>(residual_filter->Copy()) : nullptr;
	}
	return res;
}

string FileScanTask::TryFromJSON(JSONValue obj) {
	string error;
	auto data_file_val = obj.GetMember("data-file");
	if (!data_file_val.IsValid()) {
		return "FileScanTask required property 'data-file' is missing";
	} else {
		error = data_file.TryFromJSON(data_file_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto delete_file_references_val = obj.GetMember("delete-file-references");
	if (delete_file_references_val.IsValid()) {
		vector<int32_t> delete_file_references_tmp;
		if (delete_file_references_val.IsArray()) {
			delete_file_references_val.IterateArray([&](JSONValue delete_file_references_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				int32_t delete_file_references_tmp_item;
				if (json_utils::IsInteger(delete_file_references_tmp_item_val)) {
					delete_file_references_tmp_item = json_utils::GetSignedInteger(delete_file_references_tmp_item_val);
				} else {
					error =
					    StringUtil::Format("FileScanTask property 'delete_file_references_tmp_item' is not of type "
					                       "'integer', found %s instead",
					                       json_utils::GetTypeDescription(delete_file_references_tmp_item_val).c_str());
					return;
				}
				delete_file_references_tmp.emplace_back(std::move(delete_file_references_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "FileScanTask property 'delete_file_references_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(delete_file_references_val).c_str());
		}
		delete_file_references = std::move(delete_file_references_tmp);
	}
	auto residual_filter_val = obj.GetMember("residual-filter");
	if (residual_filter_val.IsValid()) {
		residual_filter = make_uniq<Predicate>();
		error = residual_filter->TryFromJSON(residual_filter_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void FileScanTask::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: data-file
	auto data_file_json = data_file.ToJSON(writer);
	obj.Add("data-file", data_file_json);

	// Serialize: delete-file-references
	if (delete_file_references.has_value()) {
		auto &delete_file_references_value = *delete_file_references;
		auto delete_file_references_json = writer.CreateArray();
		for (const auto &delete_file_references_json_item : delete_file_references_value) {
			auto delete_file_references_json_item_json = writer.CreateSignedInteger(delete_file_references_json_item);
			delete_file_references_json.Append(delete_file_references_json_item_json);
		}
		obj.Add("delete-file-references", delete_file_references_json);
	}

	// Serialize: residual-filter
	if (residual_filter != nullptr) {
		auto residual_filter_json = residual_filter->ToJSON(writer);
		obj.Add("residual-filter", residual_filter_json);
	}
}

JSONMutableValue FileScanTask::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
