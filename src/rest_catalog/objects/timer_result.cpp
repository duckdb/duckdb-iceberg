
#include "rest_catalog/objects/timer_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

TimerResult::TimerResult() {
}

TimerResult TimerResult::FromJSON(JSONValue obj) {
	TimerResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

TimerResult TimerResult::Copy() const {
	TimerResult res;
	res.time_unit = time_unit;
	res.count = count;
	res.total_duration = total_duration;
	return res;
}

string TimerResult::TryFromJSON(JSONValue obj) {
	string error;
	auto time_unit_val = obj.GetMember("time-unit");
	if (!time_unit_val.IsValid()) {
		return "TimerResult required property 'time-unit' is missing";
	} else {
		if (json_utils::IsString(time_unit_val)) {
			time_unit = json_utils::GetString(time_unit_val);
		} else {
			return StringUtil::Format("TimerResult property 'time_unit' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(time_unit_val).c_str());
		}
	}
	auto count_val = obj.GetMember("count");
	if (!count_val.IsValid()) {
		return "TimerResult required property 'count' is missing";
	} else {
		if (json_utils::IsInteger(count_val)) {
			count = json_utils::GetSignedInteger(count_val);
		} else if (json_utils::IsUnsignedInteger(count_val)) {
			count = json_utils::GetUnsignedInteger(count_val);
		} else {
			return StringUtil::Format("TimerResult property 'count' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(count_val).c_str());
		}
	}
	auto total_duration_val = obj.GetMember("total-duration");
	if (!total_duration_val.IsValid()) {
		return "TimerResult required property 'total-duration' is missing";
	} else {
		if (json_utils::IsInteger(total_duration_val)) {
			total_duration = json_utils::GetSignedInteger(total_duration_val);
		} else if (json_utils::IsUnsignedInteger(total_duration_val)) {
			total_duration = json_utils::GetUnsignedInteger(total_duration_val);
		} else {
			return StringUtil::Format(
			    "TimerResult property 'total_duration' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(total_duration_val).c_str());
		}
	}
	return "";
}

void TimerResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: time-unit
	auto time_unit_json = writer.CreateString(time_unit);
	obj.Add("time-unit", time_unit_json);

	// Serialize: count
	auto count_json = writer.CreateSignedInteger(count);
	obj.Add("count", count_json);

	// Serialize: total-duration
	auto total_duration_json = writer.CreateSignedInteger(total_duration);
	obj.Add("total-duration", total_duration_json);
}

JSONMutableValue TimerResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
