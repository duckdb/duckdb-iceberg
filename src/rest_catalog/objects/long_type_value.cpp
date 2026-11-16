
#include "rest_catalog/objects/long_type_value.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

LongTypeValue::LongTypeValue() {
}

LongTypeValue LongTypeValue::FromJSON(JSONValue obj) {
	LongTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

LongTypeValue LongTypeValue::Copy() const {
	LongTypeValue res;
	res.value = value;
	return res;
}

string LongTypeValue::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsInteger(obj)) {
		value = json_utils::GetSignedInteger(obj);
	} else if (json_utils::IsUnsignedInteger(obj)) {
		value = json_utils::GetUnsignedInteger(obj);
	} else {
		return StringUtil::Format("LongTypeValue property 'value' is not of type 'integer', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue LongTypeValue::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateSignedInteger(value);
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
