
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
		has_storage_credentials = true;
		if (yyjson_is_arr(storage_credentials_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(storage_credentials_val, idx, max, val) {
				StorageCredential tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				storage_credentials.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "Object5 property 'storage_credentials' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(storage_credentials_val));
		}
	}
	return string();
}

CompletedPlanningResult CompletedPlanningResult::FromJSON(yyjson_val *obj) {
	CompletedPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
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
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
