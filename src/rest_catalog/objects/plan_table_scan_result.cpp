
#include "rest_catalog/objects/plan_table_scan_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PlanTableScanResult::PlanTableScanResult() {
}

PlanTableScanResult PlanTableScanResult::FromJSON(yyjson_val *obj) {
	PlanTableScanResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

PlanTableScanResult PlanTableScanResult::Copy() const {
	PlanTableScanResult res;
	if (has_completed_planning_with_idresult) {
		res.completed_planning_with_idresult = completed_planning_with_idresult.Copy();
	}
	res.has_completed_planning_with_idresult = has_completed_planning_with_idresult;
	if (has_failed_planning_result) {
		res.failed_planning_result = failed_planning_result.Copy();
	}
	res.has_failed_planning_result = has_failed_planning_result;
	if (has_async_planning_result) {
		res.async_planning_result = async_planning_result.Copy();
	}
	res.has_async_planning_result = has_async_planning_result;
	if (has_empty_planning_result) {
		res.empty_planning_result = empty_planning_result.Copy();
	}
	res.has_empty_planning_result = has_empty_planning_result;
	return res;
}
string PlanTableScanResult::TryFromJSON(yyjson_val *obj) {
	string error;
	do {
		error = completed_planning_with_idresult.TryFromJSON(obj);
		if (error.empty()) {
			has_completed_planning_with_idresult = true;
			break;
		}
		error = failed_planning_result.TryFromJSON(obj);
		if (error.empty()) {
			has_failed_planning_result = true;
			break;
		}
		error = async_planning_result.TryFromJSON(obj);
		if (error.empty()) {
			has_async_planning_result = true;
			break;
		}
		error = empty_planning_result.TryFromJSON(obj);
		if (error.empty()) {
			has_empty_planning_result = true;
			break;
		}
		return "PlanTableScanResult failed to parse, none of the oneOf candidates matched";
	} while (false);
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
