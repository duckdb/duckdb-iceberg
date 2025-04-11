
#include "rest_catalog/objects/timer_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

TimerResult::TimerResult() {
}

TimerResult TimerResult::FromJSON(yyjson_val *obj) {
	TimerResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string TimerResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto time_unit_val = yyjson_obj_get(obj, "time-unit");
	if (!time_unit_val) {
		return "TimerResult required property 'time-unit' is missing";
	} else {
		time_unit = yyjson_get_str(time_unit_val);
	}
	auto count_val = yyjson_obj_get(obj, "count");
	if (!count_val) {
		return "TimerResult required property 'count' is missing";
	} else {
		count = yyjson_get_sint(count_val);
	}
	auto total_duration_val = yyjson_obj_get(obj, "total-duration");
	if (!total_duration_val) {
		return "TimerResult required property 'total-duration' is missing";
	} else {
		total_duration = yyjson_get_sint(total_duration_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
