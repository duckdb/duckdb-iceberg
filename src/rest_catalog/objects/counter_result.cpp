
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
		unit = yyjson_get_str(unit_val);
	}
	auto value_val = yyjson_obj_get(obj, "value");
	if (!value_val) {
		return "CounterResult required property 'value' is missing";
	} else {
		value = yyjson_get_sint(value_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
