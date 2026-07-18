
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
	if (null_type_value.has_value()) {
		res.null_type_value.emplace();
		(*res.null_type_value) = (*null_type_value).Copy();
	}
	if (boolean_type_value.has_value()) {
		res.boolean_type_value.emplace();
		(*res.boolean_type_value) = (*boolean_type_value).Copy();
	}
	if (integer_type_value.has_value()) {
		res.integer_type_value.emplace();
		(*res.integer_type_value) = (*integer_type_value).Copy();
	}
	if (long_type_value.has_value()) {
		res.long_type_value.emplace();
		(*res.long_type_value) = (*long_type_value).Copy();
	}
	if (float_type_value.has_value()) {
		res.float_type_value.emplace();
		(*res.float_type_value) = (*float_type_value).Copy();
	}
	if (double_type_value.has_value()) {
		res.double_type_value.emplace();
		(*res.double_type_value) = (*double_type_value).Copy();
	}
	if (decimal_type_value.has_value()) {
		res.decimal_type_value.emplace();
		(*res.decimal_type_value) = (*decimal_type_value).Copy();
	}
	if (string_type_value.has_value()) {
		res.string_type_value.emplace();
		(*res.string_type_value) = (*string_type_value).Copy();
	}
	if (uuidtype_value.has_value()) {
		res.uuidtype_value.emplace();
		(*res.uuidtype_value) = (*uuidtype_value).Copy();
	}
	if (date_type_value.has_value()) {
		res.date_type_value.emplace();
		(*res.date_type_value) = (*date_type_value).Copy();
	}
	if (time_type_value.has_value()) {
		res.time_type_value.emplace();
		(*res.time_type_value) = (*time_type_value).Copy();
	}
	if (timestamp_type_value.has_value()) {
		res.timestamp_type_value.emplace();
		(*res.timestamp_type_value) = (*timestamp_type_value).Copy();
	}
	if (timestamp_tz_type_value.has_value()) {
		res.timestamp_tz_type_value.emplace();
		(*res.timestamp_tz_type_value) = (*timestamp_tz_type_value).Copy();
	}
	if (timestamp_nano_type_value.has_value()) {
		res.timestamp_nano_type_value.emplace();
		(*res.timestamp_nano_type_value) = (*timestamp_nano_type_value).Copy();
	}
	if (timestamp_tz_nano_type_value.has_value()) {
		res.timestamp_tz_nano_type_value.emplace();
		(*res.timestamp_tz_nano_type_value) = (*timestamp_tz_nano_type_value).Copy();
	}
	if (fixed_type_value.has_value()) {
		res.fixed_type_value.emplace();
		(*res.fixed_type_value) = (*fixed_type_value).Copy();
	}
	if (binary_type_value.has_value()) {
		res.binary_type_value.emplace();
		(*res.binary_type_value) = (*binary_type_value).Copy();
	}
	return res;
}

string PrimitiveTypeValue::TryFromJSON(yyjson_val *obj) {
	string error;
	null_type_value.emplace();
	error = null_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		null_type_value = nullopt;
	}
	boolean_type_value.emplace();
	error = boolean_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		boolean_type_value = nullopt;
	}
	integer_type_value.emplace();
	error = integer_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		integer_type_value = nullopt;
	}
	long_type_value.emplace();
	error = long_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		long_type_value = nullopt;
	}
	float_type_value.emplace();
	error = float_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		float_type_value = nullopt;
	}
	double_type_value.emplace();
	error = double_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		double_type_value = nullopt;
	}
	decimal_type_value.emplace();
	error = decimal_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		decimal_type_value = nullopt;
	}
	string_type_value.emplace();
	error = string_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		string_type_value = nullopt;
	}
	uuidtype_value.emplace();
	error = uuidtype_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		uuidtype_value = nullopt;
	}
	date_type_value.emplace();
	error = date_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		date_type_value = nullopt;
	}
	time_type_value.emplace();
	error = time_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		time_type_value = nullopt;
	}
	timestamp_type_value.emplace();
	error = timestamp_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		timestamp_type_value = nullopt;
	}
	timestamp_tz_type_value.emplace();
	error = timestamp_tz_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		timestamp_tz_type_value = nullopt;
	}
	timestamp_nano_type_value.emplace();
	error = timestamp_nano_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		timestamp_nano_type_value = nullopt;
	}
	timestamp_tz_nano_type_value.emplace();
	error = timestamp_tz_nano_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		timestamp_tz_nano_type_value = nullopt;
	}
	fixed_type_value.emplace();
	error = fixed_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		fixed_type_value = nullopt;
	}
	binary_type_value.emplace();
	error = binary_type_value->TryFromJSON(obj);
	if (error.empty()) {
	} else {
		binary_type_value = nullopt;
	}
	if (!(binary_type_value.has_value()) && !(boolean_type_value.has_value()) && !(date_type_value.has_value()) &&
	    !(decimal_type_value.has_value()) && !(double_type_value.has_value()) && !(fixed_type_value.has_value()) &&
	    !(float_type_value.has_value()) && !(integer_type_value.has_value()) && !(long_type_value.has_value()) &&
	    !(null_type_value.has_value()) && !(string_type_value.has_value()) && !(time_type_value.has_value()) &&
	    !(timestamp_nano_type_value.has_value()) && !(timestamp_type_value.has_value()) &&
	    !(timestamp_tz_nano_type_value.has_value()) && !(timestamp_tz_type_value.has_value()) &&
	    !(uuidtype_value.has_value())) {
		return "PrimitiveTypeValue failed to parse, none of the anyOf candidates matched";
	}
	return "";
}

yyjson_mut_val *PrimitiveTypeValue::ToJSON(yyjson_mut_doc *doc) const {
	if (long_type_value.has_value()) {
		return long_type_value->ToJSON(doc);
	} else if (double_type_value.has_value()) {
		return double_type_value->ToJSON(doc);
	} else if (integer_type_value.has_value()) {
		return integer_type_value->ToJSON(doc);
	} else if (float_type_value.has_value()) {
		return float_type_value->ToJSON(doc);
	} else if (null_type_value.has_value()) {
		return null_type_value->ToJSON(doc);
	} else if (boolean_type_value.has_value()) {
		return boolean_type_value->ToJSON(doc);
	} else if (decimal_type_value.has_value()) {
		return decimal_type_value->ToJSON(doc);
	} else if (string_type_value.has_value()) {
		return string_type_value->ToJSON(doc);
	} else if (uuidtype_value.has_value()) {
		return uuidtype_value->ToJSON(doc);
	} else if (date_type_value.has_value()) {
		return date_type_value->ToJSON(doc);
	} else if (time_type_value.has_value()) {
		return time_type_value->ToJSON(doc);
	} else if (timestamp_type_value.has_value()) {
		return timestamp_type_value->ToJSON(doc);
	} else if (timestamp_tz_type_value.has_value()) {
		return timestamp_tz_type_value->ToJSON(doc);
	} else if (timestamp_nano_type_value.has_value()) {
		return timestamp_nano_type_value->ToJSON(doc);
	} else if (timestamp_tz_nano_type_value.has_value()) {
		return timestamp_tz_nano_type_value->ToJSON(doc);
	} else if (fixed_type_value.has_value()) {
		return fixed_type_value->ToJSON(doc);
	} else if (binary_type_value.has_value()) {
		return binary_type_value->ToJSON(doc);
	}
	// No variant is active - return null
	return yyjson_mut_null(doc);
}

} // namespace rest_api_objects
} // namespace duckdb
