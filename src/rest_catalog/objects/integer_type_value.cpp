
#include "rest_catalog/objects/integer_type_value.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

IntegerTypeValue::IntegerTypeValue() {
}

IntegerTypeValue IntegerTypeValue::FromJSON(JSONValue obj) {
	IntegerTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

IntegerTypeValue IntegerTypeValue::Copy() const {
	IntegerTypeValue res;
	res.value = value;
	return res;
}

string IntegerTypeValue::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsInteger(obj)) {
		value = json_utils::GetSignedInteger(obj);
	} else {
		return StringUtil::Format("IntegerTypeValue property 'value' is not of type 'integer', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue IntegerTypeValue::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateSignedInteger(value);
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
