
#include "rest_catalog/objects/completed_planning_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CompletedPlanningResult::CompletedPlanningResult() {
}
CompletedPlanningResult::Object5::Object5() {
}

CompletedPlanningResult::Object5 CompletedPlanningResult::Object5::FromJSON(yyjson_val *obj) {
	Object5 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CompletedPlanningResult::Object5 CompletedPlanningResult::Object5::Copy() const {
	Object5 res;
	res.status = status.Copy();
	if (storage_credentials.has_value()) {
		res.storage_credentials.emplace();
		(*res.storage_credentials).reserve((*storage_credentials).size());
		for (auto &item : (*storage_credentials)) {
			(*res.storage_credentials).emplace_back(item.Copy());
		}
	}
	return res;
}

string CompletedPlanningResult::Object5::TryFromJSON(yyjson_val *obj) {
	string error;
	auto status_val = yyjson_obj_get(obj, "status");
	if (!status_val) {
		return "Object5 required property 'status' is missing";
	} else {
		error = status.TryFromJSON(status_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto storage_credentials_val = yyjson_obj_get(obj, "storage-credentials");
	if (storage_credentials_val) {
		vector<StorageCredential> storage_credentials_tmp;
		if (yyjson_is_arr(storage_credentials_val)) {
			size_t storage_credentials_tmp_idx, storage_credentials_tmp_max;
			yyjson_val *storage_credentials_tmp_item_val;
			yyjson_arr_foreach(storage_credentials_val, storage_credentials_tmp_idx, storage_credentials_tmp_max,
			                   storage_credentials_tmp_item_val) {
				StorageCredential storage_credentials_tmp_item;
				error = storage_credentials_tmp_item.TryFromJSON(storage_credentials_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				storage_credentials_tmp.emplace_back(std::move(storage_credentials_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "Object5 property 'storage_credentials_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(storage_credentials_val));
		}
		storage_credentials = std::move(storage_credentials_tmp);
	}
	return "";
}

void CompletedPlanningResult::Object5::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: status
	yyjson_mut_val *status_val = status.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "status", status_val);

	// Serialize: storage-credentials
	if (storage_credentials.has_value()) {
		auto &storage_credentials_value = *storage_credentials;
		yyjson_mut_val *storage_credentials_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : storage_credentials_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(storage_credentials_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "storage-credentials", storage_credentials_value_arr);
	}
}

yyjson_mut_val *CompletedPlanningResult::Object5::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

CompletedPlanningResult CompletedPlanningResult::FromJSON(yyjson_val *obj) {
	CompletedPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CompletedPlanningResult CompletedPlanningResult::Copy() const {
	CompletedPlanningResult res;
	res.scan_tasks = scan_tasks.Copy();
	res.object_5 = object_5.Copy();
	return res;
}

string CompletedPlanningResult::TryFromJSON(yyjson_val *obj) {
	string error;
	error = scan_tasks.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = object_5.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return "";
}

void CompletedPlanningResult::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize base class: ScanTasks
	scan_tasks.PopulateJSON(doc, obj);

	// Serialize base class: Object5
	object_5.PopulateJSON(doc, obj);
}

yyjson_mut_val *CompletedPlanningResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
