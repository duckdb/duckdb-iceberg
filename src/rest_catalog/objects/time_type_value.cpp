
#include "rest_catalog/objects/time_type_value.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

TimeTypeValue::TimeTypeValue() {
}

TimeTypeValue TimeTypeValue::FromJSON(JSONValue obj) {
	TimeTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

TimeTypeValue TimeTypeValue::Copy() const {
	TimeTypeValue res;
	res.value = value;
	return res;
}

string TimeTypeValue::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsString(obj)) {
		value = json_utils::GetString(obj);
	} else {
		return StringUtil::Format("TimeTypeValue property 'value' is not of type 'string', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue TimeTypeValue::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateString(value);
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
