
#include "rest_catalog/objects/timestamp_tz_nano_type_value.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

TimestampTzNanoTypeValue::TimestampTzNanoTypeValue() {
}

TimestampTzNanoTypeValue TimestampTzNanoTypeValue::FromJSON(yyjson_val *obj) {
	TimestampTzNanoTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string TimestampTzNanoTypeValue::TryFromJSON(yyjson_val *obj) {
	string error;
	if (yyjson_is_str(obj)) {
		value = yyjson_get_str(obj);
	} else {
		return StringUtil::Format(
		    "TimestampTzNanoTypeValue property 'value' is not of type 'string', found '%s' instead",
		    yyjson_get_type_desc(obj));
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
