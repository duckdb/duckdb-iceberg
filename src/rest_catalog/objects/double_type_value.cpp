
#include "rest_catalog/objects/double_type_value.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

DoubleTypeValue::DoubleTypeValue() {
}

DoubleTypeValue DoubleTypeValue::FromJSON(JSONValue obj) {
	DoubleTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

DoubleTypeValue DoubleTypeValue::Copy() const {
	DoubleTypeValue res;
	res.value = value;
	return res;
}

string DoubleTypeValue::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsNumber(obj)) {
		value = json_utils::GetNumber(obj);
	} else {
		return StringUtil::Format("DoubleTypeValue property 'value' is not of type 'number', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue DoubleTypeValue::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateDouble(value);
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
