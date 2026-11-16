
#include "rest_catalog/objects/fetch_planning_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FetchPlanningResult::FetchPlanningResult() {
}

FetchPlanningResult FetchPlanningResult::FromJSON(JSONValue obj) {
	FetchPlanningResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FetchPlanningResult FetchPlanningResult::Copy() const {
	FetchPlanningResult res;
	if (completed_planning_result.has_value()) {
		res.completed_planning_result.emplace();
		(*res.completed_planning_result) = (*completed_planning_result).Copy();
	}
	if (failed_planning_result.has_value()) {
		res.failed_planning_result.emplace();
		(*res.failed_planning_result) = (*failed_planning_result).Copy();
	}
	if (empty_planning_result.has_value()) {
		res.empty_planning_result.emplace();
		(*res.empty_planning_result) = (*empty_planning_result).Copy();
	}
	return res;
}

string FetchPlanningResult::TryFromJSON(JSONValue obj) {
	string error;
	auto discriminator_val = obj.GetMember("status");
	if (!discriminator_val.IsValid() || !discriminator_val.IsString()) {
		return "FetchPlanningResult discriminator 'status' is missing or is not a string";
	}
	string discriminator = discriminator_val.GetString();
	if (discriminator == "completed") {
		completed_planning_result.emplace();
		error = completed_planning_result->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "failed") {
		failed_planning_result.emplace();
		error = failed_planning_result->TryFromJSON(obj);
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
		return StringUtil::Format("FetchPlanningResult has unknown discriminator value '%s'", discriminator.c_str());
	}
	return "";
}

void FetchPlanningResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	if (completed_planning_result.has_value()) {
		completed_planning_result->PopulateJSON(writer, obj);
	} else if (failed_planning_result.has_value()) {
		failed_planning_result->PopulateJSON(writer, obj);
	} else if (empty_planning_result.has_value()) {
		empty_planning_result->PopulateJSON(writer, obj);
	}
}

JSONMutableValue FetchPlanningResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
