
#include "rest_catalog/objects/file_scan_task.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FileScanTask::FileScanTask() {
}

FileScanTask FileScanTask::FromJSON(yyjson_val *obj) {
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
		res.residual_filter = residual_filter ? make_uniq<Expression>(residual_filter->Copy()) : nullptr;
	}
	return res;
}

string FileScanTask::TryFromJSON(yyjson_val *obj) {
	string error;
	auto data_file_val = yyjson_obj_get(obj, "data-file");
	if (!data_file_val) {
		return "FileScanTask required property 'data-file' is missing";
	} else {
		error = data_file.TryFromJSON(data_file_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto delete_file_references_val = yyjson_obj_get(obj, "delete-file-references");
	if (delete_file_references_val) {
		vector<int32_t> delete_file_references_tmp;
		if (yyjson_is_arr(delete_file_references_val)) {
			size_t delete_file_references_tmp_idx, delete_file_references_tmp_max;
			yyjson_val *delete_file_references_tmp_item_val;
			yyjson_arr_foreach(delete_file_references_val, delete_file_references_tmp_idx,
			                   delete_file_references_tmp_max, delete_file_references_tmp_item_val) {
				int32_t delete_file_references_tmp_item;
				if (yyjson_is_int(delete_file_references_tmp_item_val)) {
					delete_file_references_tmp_item = yyjson_get_int(delete_file_references_tmp_item_val);
				} else {
					return StringUtil::Format("FileScanTask property 'delete_file_references_tmp_item' is not of type "
					                          "'integer', found '%s' instead",
					                          yyjson_get_type_desc(delete_file_references_tmp_item_val));
				}
				delete_file_references_tmp.emplace_back(std::move(delete_file_references_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "FileScanTask property 'delete_file_references_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(delete_file_references_val));
		}
		delete_file_references = std::move(delete_file_references_tmp);
	}
	auto residual_filter_val = yyjson_obj_get(obj, "residual-filter");
	if (residual_filter_val) {
		residual_filter = make_uniq<Expression>();
		error = residual_filter->TryFromJSON(residual_filter_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void FileScanTask::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: data-file
	yyjson_mut_val *data_file_val = data_file.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "data-file", data_file_val);

	// Serialize: delete-file-references
	if (delete_file_references.has_value()) {
		auto &delete_file_references_value = *delete_file_references;
		yyjson_mut_val *delete_file_references_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : delete_file_references_value) {
			yyjson_mut_val *item_val = yyjson_mut_int(doc, item);
			yyjson_mut_arr_append(delete_file_references_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "delete-file-references", delete_file_references_value_arr);
	}

	// Serialize: residual-filter
	if (residual_filter != nullptr) {
		yyjson_mut_val *residual_filter_val = residual_filter->ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "residual-filter", residual_filter_val);
	}
}

yyjson_mut_val *FileScanTask::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
