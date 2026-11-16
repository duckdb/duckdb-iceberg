
#include "rest_catalog/objects/null_order.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

NullOrder::NullOrder() {
}

NullOrder NullOrder::FromJSON(JSONValue obj) {
	NullOrder res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

NullOrder NullOrder::Copy() const {
	NullOrder res;
	res.value = value;
	return res;
}

string NullOrder::TryFromJSON(JSONValue obj) {
	string error;
	if (json_utils::IsString(obj)) {
		value = json_utils::GetString(obj);
	} else {
		return StringUtil::Format("NullOrder property 'value' is not of type 'string', found %s instead",
		                          json_utils::GetTypeDescription(obj).c_str());
	}
	return "";
}

JSONMutableValue NullOrder::ToJSON(JSONWriter &writer) const {
	auto result = writer.CreateString(value);
	return result;
}

} // namespace rest_api_objects
} // namespace duckdb
