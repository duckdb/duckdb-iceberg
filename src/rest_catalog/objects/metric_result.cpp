
#include "rest_catalog/objects/metric_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

MetricResult::MetricResult() {
}

MetricResult MetricResult::FromJSON(JSONValue obj) {
	MetricResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

MetricResult MetricResult::Copy() const {
	MetricResult res;
	if (counter_result.has_value()) {
		res.counter_result.emplace();
		(*res.counter_result) = (*counter_result).Copy();
	}
	if (timer_result.has_value()) {
		res.timer_result.emplace();
		(*res.timer_result) = (*timer_result).Copy();
	}
	return res;
}

string MetricResult::TryFromJSON(JSONValue obj) {
	string error;
	counter_result.emplace();
	error = counter_result->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		counter_result = nullopt;
	}
	timer_result.emplace();
	error = timer_result->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		timer_result = nullopt;
	}
	if (!(counter_result.has_value()) && !(timer_result.has_value())) {
		return "MetricResult failed to parse, none of the anyOf candidates matched";
	}
	return "";
}

void MetricResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	if (counter_result.has_value()) {
		counter_result->PopulateJSON(writer, obj);
	} else if (timer_result.has_value()) {
		timer_result->PopulateJSON(writer, obj);
	}
}

JSONMutableValue MetricResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
