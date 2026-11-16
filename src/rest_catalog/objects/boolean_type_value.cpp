
#include "rest_catalog/objects/boolean_type_value.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

BooleanTypeValue::BooleanTypeValue() {
}

BooleanTypeValue BooleanTypeValue::FromJSON(JSONValue obj) {
	BooleanTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

BooleanTypeValue BooleanTypeValue::Copy() const {
	BooleanTypeValue res;
	res.value = value;
	return res;
}

string BooleanTypeValue::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsBoolean(obj)) {
		value = json_utils::GetBoolean(obj);
	} else {
		return StringUtil::Format("BooleanTypeValue property 'value' is not of type 'boolean', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue BooleanTypeValue::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateBoolean(value);
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
