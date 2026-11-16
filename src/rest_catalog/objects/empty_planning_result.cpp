
#include "rest_catalog/objects/empty_planning_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

EmptyPlanningResult::EmptyPlanningResult() {
}

EmptyPlanningResult EmptyPlanningResult::FromJSON(JSONValue obj) {
	EmptyPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

EmptyPlanningResult EmptyPlanningResult::Copy() const {
	EmptyPlanningResult res;
	res.status = status.Copy();
	return res;
}

string EmptyPlanningResult::TryFromJSON(JSONValue obj) {
	string error;
	auto status_val = obj.GetMember("status");
	if (!status_val.IsValid()) {
		return "EmptyPlanningResult required property 'status' is missing";
	} else {
		error = status.TryFromJSON(status_val);
		if (!error.empty()) {
			return error;
		}
	}
	return "";
}

void EmptyPlanningResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: status
	auto status_json = status.ToJSON(writer);
	obj.Add("status", status_json);
}

JSONMutableValue EmptyPlanningResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
