
#include "rest_catalog/objects/primitive_type_value.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PrimitiveTypeValue::PrimitiveTypeValue() {
}

PrimitiveTypeValue PrimitiveTypeValue::FromJSON(yyjson_val *obj) {
	PrimitiveTypeValue res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string PrimitiveTypeValue::TryFromJSON(yyjson_val *obj) {
	string error;
	error = boolean_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_boolean_type_value = true;
	}
	error = integer_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_integer_type_value = true;
	}
	error = long_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_long_type_value = true;
	}
	error = float_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_float_type_value = true;
	}
	error = double_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_double_type_value = true;
	}
	error = decimal_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_decimal_type_value = true;
	}
	error = string_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_string_type_value = true;
	}
	error = uuidtype_value.TryFromJSON(obj);
	if (error.empty()) {
		has_uuidtype_value = true;
	}
	error = date_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_date_type_value = true;
	}
	error = time_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_time_type_value = true;
	}
	error = timestamp_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_timestamp_type_value = true;
	}
	error = timestamp_tz_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_timestamp_tz_type_value = true;
	}
	error = timestamp_nano_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_timestamp_nano_type_value = true;
	}
	error = timestamp_tz_nano_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_timestamp_tz_nano_type_value = true;
	}
	error = fixed_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_fixed_type_value = true;
	}
	error = binary_type_value.TryFromJSON(obj);
	if (error.empty()) {
		has_binary_type_value = true;
	}
	if (!has_binary_type_value && !has_boolean_type_value && !has_date_type_value && !has_decimal_type_value &&
	    !has_double_type_value && !has_fixed_type_value && !has_float_type_value && !has_integer_type_value &&
	    !has_long_type_value && !has_string_type_value && !has_time_type_value && !has_timestamp_nano_type_value &&
	    !has_timestamp_type_value && !has_timestamp_tz_nano_type_value && !has_timestamp_tz_type_value &&
	    !has_uuidtype_value) {
		return "PrimitiveTypeValue failed to parse, none of the anyOf candidates matched";
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
