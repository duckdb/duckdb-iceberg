
#include "rest_catalog/objects/counter_result.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CounterResult::CounterResult() {
}

CounterResult CounterResult::FromJSON(yyjson_val *obj) {
	CounterResult res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string CounterResult::TryFromJSON(yyjson_val *obj) {
	string error;
	auto unit_val = yyjson_obj_get(obj, "unit");
	if (!unit_val) {
		return "CounterResult required property 'unit' is missing";
	} else {
		if (yyjson_is_str(unit_val)) {
			unit = yyjson_get_str(unit_val);
		} else {
			return StringUtil::Format("CounterResult property 'unit' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(unit_val));
		}
	}
	auto value_val = yyjson_obj_get(obj, "value");
	if (!value_val) {
		return "CounterResult required property 'value' is missing";
	} else {
		if (yyjson_is_int(value_val)) {
			value = yyjson_get_int(value_val);
		} else {
			return StringUtil::Format("CounterResult property 'value' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(value_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
