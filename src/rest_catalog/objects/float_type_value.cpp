
#include "rest_catalog/objects/float_type_value.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

FloatTypeValue::FloatTypeValue() {
}

FloatTypeValue FloatTypeValue::FromJSON(JSONValue obj) {
	FloatTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

FloatTypeValue FloatTypeValue::Copy() const {
	FloatTypeValue res;
	res.value = value;
	return res;
}

string FloatTypeValue::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsNumber(obj)) {
		value = json_utils::GetNumber(obj);
	} else {
		return StringUtil::Format("FloatTypeValue property 'value' is not of type 'number', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue FloatTypeValue::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateDouble(value);
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
