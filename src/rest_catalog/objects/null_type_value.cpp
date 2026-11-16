
#include "rest_catalog/objects/null_type_value.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

NullTypeValue::NullTypeValue() {
}

NullTypeValue NullTypeValue::FromJSON(JSONValue obj) {
	NullTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

NullTypeValue NullTypeValue::Copy() const {
	NullTypeValue res;
	res.value = value;
	return res;
}

string NullTypeValue::TryFromJSON(JSONValue obj) {
	string error;
	if (obj.IsNull()) {
		//! do nothing, property is explicitly nullable
	} else if (json_utils::IsNull(obj)) {
		value = json_utils::GetNull(obj);
	} else {
		return StringUtil::Format("NullTypeValue property 'value' is not of type 'None', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue NullTypeValue::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateNull();
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
