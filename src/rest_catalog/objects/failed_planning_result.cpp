
#include "rest_catalog/objects/failed_planning_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FailedPlanningResult::FailedPlanningResult() {
}
FailedPlanningResult::Object12::Object12() {
}

FailedPlanningResult::Object12 FailedPlanningResult::Object12::FromJSON(JSONValue obj) {
	Object12 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FailedPlanningResult::Object12 FailedPlanningResult::Object12::Copy() const {
	Object12 res;
	res.status = status.Copy();
	return res;
}

string FailedPlanningResult::Object12::TryFromJSON(JSONValue obj) {
	string error;
	auto status_val = obj.GetMember("status");
	if (!status_val.IsValid()) {
		return "Object12 required property 'status' is missing";
	} else {
		error = status.TryFromJSON(status_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void FailedPlanningResult::Object12::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: status
	auto status_json = status.ToJSON(writer);
	obj.Add("status", status_json);
}

JSONMutableValue FailedPlanningResult::Object12::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

FailedPlanningResult FailedPlanningResult::FromJSON(JSONValue obj) {
	FailedPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FailedPlanningResult FailedPlanningResult::Copy() const {
	FailedPlanningResult res;
	res.iceberg_error_response = iceberg_error_response.Copy();
	res.object_12 = object_12.Copy();
	return res;
}

string FailedPlanningResult::TryFromJSON(JSONValue obj) {
	string error;
	error = iceberg_error_response.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	error = object_12.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	return "";
}

void FailedPlanningResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: IcebergErrorResponse
	iceberg_error_response.PopulateJSON(writer, obj);

	// Serialize base class: Object12
	object_12.PopulateJSON(writer, obj);
}

JSONMutableValue FailedPlanningResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
