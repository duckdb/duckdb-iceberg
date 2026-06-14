
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

PrimitiveTypeValue PrimitiveTypeValue::Copy() const {
	PrimitiveTypeValue res;
	if (has_boolean_type_value) {
		res.boolean_type_value = boolean_type_value.Copy();
	}
	res.has_boolean_type_value = has_boolean_type_value;
	if (has_integer_type_value) {
		res.integer_type_value = integer_type_value.Copy();
	}
	res.has_integer_type_value = has_integer_type_value;
	if (has_long_type_value) {
		res.long_type_value = long_type_value.Copy();
	}
	res.has_long_type_value = has_long_type_value;
	if (has_float_type_value) {
		res.float_type_value = float_type_value.Copy();
	}
	res.has_float_type_value = has_float_type_value;
	if (has_double_type_value) {
		res.double_type_value = double_type_value.Copy();
	}
	res.has_double_type_value = has_double_type_value;
	if (has_decimal_type_value) {
		res.decimal_type_value = decimal_type_value.Copy();
	}
	res.has_decimal_type_value = has_decimal_type_value;
	if (has_string_type_value) {
		res.string_type_value = string_type_value.Copy();
	}
	res.has_string_type_value = has_string_type_value;
	if (has_uuidtype_value) {
		res.uuidtype_value = uuidtype_value.Copy();
	}
	res.has_uuidtype_value = has_uuidtype_value;
	if (has_date_type_value) {
		res.date_type_value = date_type_value.Copy();
	}
	res.has_date_type_value = has_date_type_value;
	if (has_time_type_value) {
		res.time_type_value = time_type_value.Copy();
	}
	res.has_time_type_value = has_time_type_value;
	if (has_timestamp_type_value) {
		res.timestamp_type_value = timestamp_type_value.Copy();
	}
	res.has_timestamp_type_value = has_timestamp_type_value;
	if (has_timestamp_tz_type_value) {
		res.timestamp_tz_type_value = timestamp_tz_type_value.Copy();
	}
	res.has_timestamp_tz_type_value = has_timestamp_tz_type_value;
	if (has_timestamp_nano_type_value) {
		res.timestamp_nano_type_value = timestamp_nano_type_value.Copy();
	}
	res.has_timestamp_nano_type_value = has_timestamp_nano_type_value;
	if (has_timestamp_tz_nano_type_value) {
		res.timestamp_tz_nano_type_value = timestamp_tz_nano_type_value.Copy();
	}
	res.has_timestamp_tz_nano_type_value = has_timestamp_tz_nano_type_value;
	if (has_fixed_type_value) {
		res.fixed_type_value = fixed_type_value.Copy();
	}
	res.has_fixed_type_value = has_fixed_type_value;
	if (has_binary_type_value) {
		res.binary_type_value = binary_type_value.Copy();
	}
	res.has_binary_type_value = has_binary_type_value;
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
