
#include "rest_catalog/objects/fetch_planning_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

FetchPlanningResult::FetchPlanningResult() {
}

FetchPlanningResult FetchPlanningResult::FromJSON(yyjson_val *obj) {
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

string FetchPlanningResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto discriminator_val = yyjson_obj_get(obj, "status");
	if (!discriminator_val || !yyjson_is_str(discriminator_val)) {
		return "FetchPlanningResult discriminator 'status' is missing or is not a string";
	}
	string discriminator = yyjson_get_str(discriminator_val);
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

void FetchPlanningResult::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	if (completed_planning_result.has_value()) {
		completed_planning_result->PopulateJSON(doc, obj);
	} else if (failed_planning_result.has_value()) {
		failed_planning_result->PopulateJSON(doc, obj);
	} else if (empty_planning_result.has_value()) {
		empty_planning_result->PopulateJSON(doc, obj);
	}
}

yyjson_mut_val *FetchPlanningResult::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
