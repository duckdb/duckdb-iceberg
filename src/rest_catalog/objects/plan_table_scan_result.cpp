
#include "rest_catalog/objects/plan_table_scan_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

PlanTableScanResult::PlanTableScanResult() {
}

PlanTableScanResult PlanTableScanResult::FromJSON(JSONValue obj) {
	PlanTableScanResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

PlanTableScanResult PlanTableScanResult::Copy() const {
	PlanTableScanResult res;
	if (completed_planning_with_idresult.has_value()) {
		res.completed_planning_with_idresult.emplace();
		(*res.completed_planning_with_idresult) = (*completed_planning_with_idresult).Copy();
	}
	if (failed_planning_result.has_value()) {
		res.failed_planning_result.emplace();
		(*res.failed_planning_result) = (*failed_planning_result).Copy();
	}
	if (async_planning_result.has_value()) {
		res.async_planning_result.emplace();
		(*res.async_planning_result) = (*async_planning_result).Copy();
	}
	if (empty_planning_result.has_value()) {
		res.empty_planning_result.emplace();
		(*res.empty_planning_result) = (*empty_planning_result).Copy();
	}
	return res;
}

string PlanTableScanResult::TryFromJSON(JSONValue obj) {
	string error;
	auto discriminator_val = obj.GetMember("status");
	if (!discriminator_val.IsValid() || !discriminator_val.IsString()) {
		return "PlanTableScanResult discriminator 'status' is missing or is not a string";
	}
	string discriminator = discriminator_val.GetString();
	if (discriminator == "completed") {
		completed_planning_with_idresult.emplace();
		error = completed_planning_with_idresult->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "failed") {
		failed_planning_result.emplace();
		error = failed_planning_result->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "submitted") {
		async_planning_result.emplace();
		error = async_planning_result->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "cancelled") {
		empty_planning_result.emplace();
		error = empty_planning_result->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else {
		return StringUtil::Format("PlanTableScanResult has unknown discriminator value '%s'", discriminator.c_str());
	}
	return "";
}

void PlanTableScanResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	if (completed_planning_with_idresult.has_value()) {
		completed_planning_with_idresult->PopulateJSON(writer, obj);
	} else if (failed_planning_result.has_value()) {
		failed_planning_result->PopulateJSON(writer, obj);
	} else if (async_planning_result.has_value()) {
		async_planning_result->PopulateJSON(writer, obj);
	} else if (empty_planning_result.has_value()) {
		empty_planning_result->PopulateJSON(writer, obj);
	}
}

JSONMutableValue PlanTableScanResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
