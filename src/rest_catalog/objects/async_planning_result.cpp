
#include "rest_catalog/objects/async_planning_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

AsyncPlanningResult::AsyncPlanningResult() {
}

AsyncPlanningResult AsyncPlanningResult::FromJSON(JSONValue obj) {
	AsyncPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

AsyncPlanningResult AsyncPlanningResult::Copy() const {
	AsyncPlanningResult res;
	res.status = status.Copy();
	res.plan_id = plan_id;
	return res;
}

string AsyncPlanningResult::TryFromJSON(JSONValue obj) {
	string error;
	auto status_val = obj.GetMember("status");
	if (!status_val.IsValid()) {
		return "AsyncPlanningResult required property 'status' is missing";
	} else {
		error = status.TryFromJSON(status_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto plan_id_val = obj.GetMember("plan-id");
	if (!plan_id_val.IsValid()) {
		return "AsyncPlanningResult required property 'plan-id' is missing";
	} else {
		if (json_utils::IsString(plan_id_val)) {
			plan_id = json_utils::GetString(plan_id_val);
		} else {
			return StringUtil::Format(
			    "AsyncPlanningResult property 'plan_id' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(plan_id_val).c_str());
		}
	}
	return "";
}

void AsyncPlanningResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: status
	auto status_json = status.ToJSON(writer);
	obj.Add("status", status_json);

	// Serialize: plan-id
	auto plan_id_json = writer.CreateString(plan_id);
	obj.Add("plan-id", plan_id_json);
}

JSONMutableValue AsyncPlanningResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
