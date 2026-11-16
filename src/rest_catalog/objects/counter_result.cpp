
#include "rest_catalog/objects/counter_result.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

CounterResult::CounterResult() {
}

CounterResult CounterResult::FromJSON(JSONValue obj) {
	CounterResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

CounterResult CounterResult::Copy() const {
	CounterResult res;
	res.unit = unit;
	res.value = value;
	return res;
}

string CounterResult::TryFromJSON(JSONValue obj) {
	string error;
	auto unit_val = obj.GetMember("unit");
	if (!unit_val.IsValid()) {
		return "CounterResult required property 'unit' is missing";
	} else {
		if (json_utils::IsString(unit_val)) {
			unit = json_utils::GetString(unit_val);
		} else {
			return StringUtil::Format("CounterResult property 'unit' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(unit_val).c_str());
		}
	}
	auto value_val = obj.GetMember("value");
	if (!value_val.IsValid()) {
		return "CounterResult required property 'value' is missing";
	} else {
		if (json_utils::IsInteger(value_val)) {
			value = json_utils::GetSignedInteger(value_val);
		} else if (json_utils::IsUnsignedInteger(value_val)) {
			value = json_utils::GetUnsignedInteger(value_val);
		} else {
			return StringUtil::Format("CounterResult property 'value' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(value_val).c_str());
		}
	}
	return "";
}

void CounterResult::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: unit
	auto unit_json = writer.CreateString(unit);
	obj.Add("unit", unit_json);

	// Serialize: value
	auto value_json = writer.CreateSignedInteger(value);
	obj.Add("value", value_json);
}

JSONMutableValue CounterResult::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
