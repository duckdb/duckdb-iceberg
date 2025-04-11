
#include "rest_catalog/objects/primitive_type_value.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
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
	do {
		error = boolean_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_boolean_type_value = true;
			break;
		}
		error = integer_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_integer_type_value = true;
			break;
		}
		error = long_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_long_type_value = true;
			break;
		}
		error = float_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_float_type_value = true;
			break;
		}
		error = double_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_double_type_value = true;
			break;
		}
		error = decimal_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_decimal_type_value = true;
			break;
		}
		error = string_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_string_type_value = true;
			break;
		}
		error = uuidtype_value.TryFromJSON(obj);
		if (error.empty()) {
			has_uuidtype_value = true;
			break;
		}
		error = date_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_date_type_value = true;
			break;
		}
		error = time_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_time_type_value = true;
			break;
		}
		error = timestamp_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_timestamp_type_value = true;
			break;
		}
		error = timestamp_tz_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_timestamp_tz_type_value = true;
			break;
		}
		error = timestamp_nano_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_timestamp_nano_type_value = true;
			break;
		}
		error = timestamp_tz_nano_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_timestamp_tz_nano_type_value = true;
			break;
		}
		error = fixed_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_fixed_type_value = true;
			break;
		}
		error = binary_type_value.TryFromJSON(obj);
		if (error.empty()) {
			has_binary_type_value = true;
			break;
		}
		return "PrimitiveTypeValue failed to parse, none of the oneOf candidates matched";
	} while (false);
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
