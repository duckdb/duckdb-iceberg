
#include "rest_catalog/objects/integer_type_value.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

IntegerTypeValue::IntegerTypeValue() {
}

IntegerTypeValue IntegerTypeValue::FromJSON(yyjson_val *obj) {
	IntegerTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string IntegerTypeValue::TryFromJSON(yyjson_val *obj) {
	string error;
	if (yyjson_is_int(obj)) {
		value = yyjson_get_int(obj);
	} else {
		return StringUtil::Format("IntegerTypeValue property 'value' is not of type 'integer', found '%s' instead",
		                          yyjson_get_type_desc(obj));
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
