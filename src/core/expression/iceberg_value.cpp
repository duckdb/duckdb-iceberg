#include "core/expression/iceberg_value.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/bswap.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

static DeserializeResult DeserializeError(const string_t &blob, const LogicalType &type) {
	return DeserializeResult(
	    StringUtil::Format("Failed to deserialize blob '%s' of size %d, attempting to produce value of type '%s'",
	                       blob.GetString(), blob.GetSize(), type.ToString()));
}

template <class VALUE_TYPE>
static Value DeserializeDecimalTemplated(const string_t &blob, uint8_t width, uint8_t scale) {
	VALUE_TYPE ret = 0;
	//! The blob has to be smaller or equal to the size of the type
	D_ASSERT(blob.GetSize() <= sizeof(VALUE_TYPE));

	// Convert from big-endian to host byte order
	const uint8_t *src = reinterpret_cast<const uint8_t *>(blob.GetData());
	for (idx_t i = 0; i < blob.GetSize(); i++) {
		ret = (ret << 8) | src[i];
	}

	// Handle sign extension for negative numbers (if high bit is set)
	if (blob.GetSize() > 0 && (src[0] & 0x80)) {
		// Fill remaining bytes with 1s for negative numbers
		idx_t shift_amount = (sizeof(VALUE_TYPE) - blob.GetSize()) * 8;
		if (shift_amount > 0) {
			// Create a mask with 1s in the upper bits that need to be filled
			VALUE_TYPE mask = ((VALUE_TYPE)1 << shift_amount) - 1;
			mask = mask << (blob.GetSize() * 8);
			ret |= mask;
		}
	}

	return Value::DECIMAL(ret, width, scale);
}

static Value DeserializeHugeintDecimal(const string_t &blob, uint8_t width, uint8_t scale) {
	//! The blob has to be smaller or equal to the size of the type
	D_ASSERT(blob.GetSize() <= sizeof(hugeint_t));

	const uint8_t *src = reinterpret_cast<const uint8_t *>(blob.GetData());
	bool is_negative = blob.GetSize() > 0 && (src[0] & 0x80);

	// use unsigned integers for shifting
	uint64_t upper_val = is_negative ? ~uint64_t(0) : 0;
	uint64_t lower_val = is_negative ? ~uint64_t(0) : 0;

	for (idx_t i = 0; i < blob.GetSize(); i++) {
		upper_val = (upper_val << 8) | (lower_val >> 56);
		lower_val = (lower_val << 8) | src[i];
	}

	hugeint_t ret(static_cast<int64_t>(upper_val), lower_val);
	return Value::DECIMAL(ret, width, scale);
}

static DeserializeResult DeserializeDecimal(const string_t &blob, const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);

	uint8_t width;
	uint8_t scale;
	if (!type.GetDecimalProperties(width, scale)) {
		return DeserializeError(blob, type);
	}

	auto physical_type = type.InternalType();
	switch (physical_type) {
	case PhysicalType::INT16: {
		return DeserializeDecimalTemplated<int16_t>(blob, width, scale);
	}
	case PhysicalType::INT32: {
		return DeserializeDecimalTemplated<int32_t>(blob, width, scale);
	}
	case PhysicalType::INT64: {
		return DeserializeDecimalTemplated<int64_t>(blob, width, scale);
	}
	case PhysicalType::INT128: {
		return DeserializeHugeintDecimal(blob, width, scale);
	}
	default:
		throw InternalException("DeserializeDecimal not implemented for physical type '%s'",
		                        TypeIdToString(physical_type));
	}
}

static DeserializeResult DeserializeUUID(const string_t &blob, const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::UUID);

	if (blob.GetSize() != sizeof(uhugeint_t)) {
		return DeserializeError(blob, type);
	}

	// Convert from big-endian to host byte order
	auto src = reinterpret_cast<const_data_ptr_t>(blob.GetData());
	uhugeint_t ret;
	ret.upper = BSwap(Load<uint64_t>(src));
	ret.lower = BSwap(Load<uint64_t>(src + sizeof(uint64_t)));
	return Value::UUID(UUID::FromUHugeint(ret));
}

//! FIXME: because of schema evolution, there are rules for inferring the correct type that we need to apply:
//! See https://iceberg.apache.org/spec/#schema-evolution
DeserializeResult IcebergValue::DeserializeValue(const string_t &blob, const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::INTEGER: {
		if (blob.GetSize() != sizeof(int32_t)) {
			return DeserializeError(blob, type);
		}
		int32_t val;
		std::memcpy(&val, blob.GetData(), sizeof(int32_t));
		return Value::INTEGER(val);
	}
	case LogicalTypeId::BIGINT: {
		if (blob.GetSize() == sizeof(int32_t)) {
			//! Schema evolution happened: Infer the type as INTEGER
			return DeserializeValue(blob, LogicalType::INTEGER);
		} else if (blob.GetSize() == sizeof(int64_t)) {
			int64_t val;
			std::memcpy(&val, blob.GetData(), sizeof(int64_t));
			return Value::BIGINT(val);
		} else {
			return DeserializeError(blob, type);
		}
	}
	case LogicalTypeId::DATE: {
		if (blob.GetSize() != sizeof(int32_t)) { // Dates are typically stored as int32 (days since epoch)
			return DeserializeError(blob, type);
		}
		int32_t days_since_epoch;
		std::memcpy(&days_since_epoch, blob.GetData(), sizeof(int32_t));
		// Convert to DuckDB date
		date_t date = Date::EpochDaysToDate(days_since_epoch);
		return Value::DATE(date);
	}
	case LogicalTypeId::TIMESTAMP: {
		if (blob.GetSize() == sizeof(int32_t)) {
			//! Schema evolution happened: Infer the type as DATE
			return DeserializeValue(blob, LogicalType::DATE);
		} else if (blob.GetSize() ==
		           sizeof(int64_t)) { // Timestamps are typically stored as int64 (microseconds since epoch)
			int64_t micros_since_epoch;
			std::memcpy(&micros_since_epoch, blob.GetData(), sizeof(int64_t));
			// Convert to DuckDB timestamp using microseconds
			timestamp_t timestamp = Timestamp::FromEpochMicroSeconds(micros_since_epoch);
			return Value::TIMESTAMP(timestamp);
		} else {
			return DeserializeError(blob, type);
		}
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		if (blob.GetSize() != sizeof(int64_t)) { // Assuming stored as int64 (microseconds since epoch)
			return DeserializeError(blob, type);
		}
		int64_t micros_since_epoch;
		std::memcpy(&micros_since_epoch, blob.GetData(), sizeof(int64_t));
		// Convert to DuckDB timestamp using microseconds
		timestamp_t timestamp = Timestamp::FromEpochMicroSeconds(micros_since_epoch);
		// Create a TIMESTAMPTZ Value
		return Value::TIMESTAMPTZ(timestamp_tz_t(timestamp));
	}
	case LogicalTypeId::DOUBLE: {
		if (blob.GetSize() == sizeof(float)) {
			//! Schema evolution happened: Infer the type as FLOAT
			return DeserializeValue(blob, LogicalType::FLOAT);
		} else if (blob.GetSize() == sizeof(double)) {
			double val;
			std::memcpy(&val, blob.GetData(), sizeof(double));
			return Value::DOUBLE(val);
		} else {
			return DeserializeError(blob, type);
		}
	}
	case LogicalTypeId::BLOB: {
		return Value::BLOB((data_ptr_t)blob.GetData(), blob.GetSize());
	}
	case LogicalTypeId::VARCHAR: {
		// Assume the bytes represent a UTF-8 string
		return Value(blob);
	}
	case LogicalTypeId::DECIMAL: {
		return DeserializeDecimal(blob, type);
	}
	case LogicalTypeId::BOOLEAN: {
		if (blob.GetSize() != 1) {
			return DeserializeError(blob, type);
		}
		const bool val = blob.GetData()[0] != '\0';
		return Value::BOOLEAN(val);
	}
	case LogicalTypeId::FLOAT: {
		if (blob.GetSize() != sizeof(float)) {
			return DeserializeError(blob, type);
		}
		float val;
		std::memcpy(&val, blob.GetData(), sizeof(float));
		return Value::FLOAT(val);
	}
	case LogicalTypeId::TIME: {
		if (blob.GetSize() != sizeof(int64_t)) {
			return DeserializeError(blob, type);
		}
		//! bound stores microseconds since midnight
		dtime_t val;
		std::memcpy(&val.value, blob.GetData(), sizeof(int64_t));
		return Value::TIME(val);
	}
	case LogicalTypeId::TIMESTAMP_NS:
		if (blob.GetSize() == sizeof(int32_t)) {
			//! Schema evolution happened: Infer the type as DATE
			return DeserializeValue(blob, LogicalType::DATE);
		} else if (blob.GetSize() ==
		           sizeof(int64_t)) { // Timestamps are typically stored as int64 (microseconds since epoch)
			int64_t nanos_since_epoch;
			std::memcpy(&nanos_since_epoch, blob.GetData(), sizeof(int64_t));
			// Convert to DuckDB timestamp using nanoseconds
			timestamp_ns_t timestamp(nanos_since_epoch);
			return Value::TIMESTAMPNS(timestamp);
		} else {
			return DeserializeError(blob, type);
		}
	case LogicalTypeId::UUID:
		return DeserializeUUID(blob, type);
	default:
		break;
	}
	return DeserializeError(blob, type);
}

const idx_t IcebergValue::MAX_STRING_UPPERBOUND_LENGTH;

// Largest length <= max_length that doesn't split a multi-byte UTF-8 character
// (INVALID_INDEX = no truncation), so a truncated bound stays valid UTF-8.
static idx_t TruncateToCodePointBoundary(const string &input, idx_t max_length) {
	if (max_length >= input.size()) {
		return input.size();
	}
	idx_t len = max_length;
	while (len < input.size() && (static_cast<unsigned char>(input[len]) & 0xC0) == 0x80) {
		len--;
	}
	return len;
}

string IcebergValue::TruncateString(const string &input, idx_t max_length) {
	// A prefix truncated on a code-point boundary is a valid lower bound (<= input).
	return input.substr(0, TruncateToCodePointBoundary(input, max_length));
}

bool IcebergValue::TruncateAndIncrementString(const string &input, string &result, idx_t max_length) {
	// If the whole value fits within the bound length it is itself a valid
	// (exact) upper bound; nothing to truncate or increment.
	if (input.size() <= max_length) {
		result = input;
		return true;
	}

	// Truncate to a code-point boundary, then round up to a valid upper bound by
	// incrementing the last code point below.
	idx_t len = TruncateToCodePointBoundary(input, max_length);

	// Round the truncated prefix up to a valid upper bound by incrementing the
	// last code point to the next scalar value (skipping the UTF-16 surrogate
	// range). This works on whole code points rather than raw bytes, so it is
	// correct for multi-byte UTF-8 (e.g. full-width or Turkish titles). If the
	// last code point is already the maximum, drop it and carry the increment to
	// the previous one. If every code point is the maximum, no representable
	// upper bound exists and the caller should omit it (it is optional).
	while (len > 0) {
		// find the start of the last code point in input[0, len)
		idx_t last_start = len - 1;
		while (last_start > 0 && (static_cast<unsigned char>(input[last_start]) & 0xC0) == 0x80) {
			last_start--;
		}
		int cp_size;
		int32_t codepoint = Utf8Proc::UTF8ToCodepoint(input.c_str() + last_start, cp_size, input.size()) + 1;
		if (codepoint >= 0xD800 && codepoint <= 0xDFFF) {
			// skip the surrogate range to the next valid scalar value
			codepoint = 0xE000;
		}
		char encoded[4];
		int encoded_size;
		if (Utf8Proc::CodepointToUtf8(codepoint, encoded_size, encoded)) {
			result = input.substr(0, last_start) + string(encoded, static_cast<size_t>(encoded_size));
			return true;
		}
		// the last code point was U+10FFFF and cannot be incremented; drop it and
		// carry the increment to the preceding code point.
		len = last_start;
	}
	return false;
}

std::vector<uint8_t> HexStringToBytes(const std::string &hex) {
	std::vector<uint8_t> bytes;
	D_ASSERT(hex.size() % 2 == 0);
	bytes.reserve(hex.size() / 2);

	for (size_t i = 0; i < hex.size(); i += 2) {
		uint8_t byte = std::stoi(hex.substr(i, 2), nullptr, 16);
		bytes.push_back(byte);
	}
	return bytes;
}

SerializeResult IcebergValue::SerializeValue(Value input_value, const LogicalType &column_type,
                                             SerializeBound bound_type, const IcebergMetricsConfig &metrics_config) {
	switch (column_type.id()) {
	case LogicalTypeId::INTEGER: {
		int32_t val = input_value.GetValue<int32_t>();
		auto serialized_const_data_ptr = const_data_ptr_cast<int32_t>(&val);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(int32_t));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::BIGINT: {
		int64_t val = input_value.GetValue<int64_t>();
		auto serialized_const_data_ptr = const_data_ptr_cast<int64_t>(&val);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(int64_t));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::VARCHAR: {
		auto input = input_value.GetValue<string>();
		string val;
		if (bound_type == SerializeBound::UPPER_BOUND) {
			// an upper bound must be truncated and rounded up to stay >= every value
			if (!TruncateAndIncrementString(input, val, metrics_config.truncate_length)) {
				// No representable upper bound could be produced; omit it (optional per spec).
				return SerializeResult();
			}
		} else {
			// a truncated prefix is already a valid lower bound (<= every value)
			val = TruncateString(input, metrics_config.truncate_length);
		}
		auto serialized_val = Value::BLOB(reinterpret_cast<const_data_ptr_t>(val.data()), val.size());
		return SerializeResult(column_type, serialized_val);
	}
	case LogicalTypeId::FLOAT: {
		float val = input_value.GetValue<float>();
		auto serialized_const_data_ptr = const_data_ptr_cast<float>(&val);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(float));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::DOUBLE: {
		double val = input_value.GetValue<double>();
		auto serialized_const_data_ptr = const_data_ptr_cast<double>(&val);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(double));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::DATE: {
		date_t val = input_value.GetValue<date_t>();
		if (val == date_t::infinity() || val == date_t::ninfinity()) {
			throw ConversionException("Cannot write infinity/-infinity for date type");
		}
		int32_t epoch_days = Date::EpochDays(val);
		auto serialized_const_data_ptr = const_data_ptr_cast<int32_t>(&epoch_days);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(int32_t));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::TIMESTAMP: {
		timestamp_t val = input_value.GetValue<timestamp_t>();
		if (val == timestamp_t::infinity() || val == timestamp_t::ninfinity()) {
			throw ConversionException("Cannot write infinity/-infinity for timestamp type");
		}
		int64_t micros_since_epoch = val.value;
		auto serialized_const_data_ptr = const_data_ptr_cast<int64_t>(&micros_since_epoch);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(int64_t));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		timestamp_tz_t val = input_value.GetValue<timestamp_tz_t>();
		if (val == timestamp_tz_t::infinity() || val == timestamp_tz_t::ninfinity()) {
			throw ConversionException("Cannot write infinity/-infinity for date type");
		}
		// Get the timestamp component (microseconds since epoch in UTC)
		int64_t micros_since_epoch = val.value;
		auto serialized_const_data_ptr = const_data_ptr_cast<int64_t>(&micros_since_epoch);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(int64_t));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::TIMESTAMP_NS: {
		timestamp_ns_t val = input_value.GetValue<timestamp_ns_t>();
		if (!Value::IsFinite(val)) {
			throw ConversionException("Cannot write infinity/-infinity for timestamp_ns type");
		}
		int64_t nanos_since_epoch = val.value;
		auto serialized_const_data_ptr = const_data_ptr_cast<int64_t>(&nanos_since_epoch);
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(int64_t));
		return SerializeResult(column_type, serialized_val);
	}
	case LogicalTypeId::DECIMAL: {
		auto decimal_as_string = input_value.GetValue<string>();
		auto dec_pos = decimal_as_string.find(".");
		// remove the decimal point if found (when scale is 0 there is no decimal point)
		if (dec_pos != string::npos) {
			decimal_as_string.erase(dec_pos, 1);
		}
		auto unscaled = Value(decimal_as_string).DefaultCastAs(LogicalType::HUGEINT);
		auto unscaled_hugeint = unscaled.GetValue<hugeint_t>();
		vector<uint8_t> big_endian_bytes;
		bool needs_positive_padding = false;
		bool needs_negative_padding = false;
		auto huge_int_bytes = sizeof(hugeint_t);
		bool first_val = false;
		bool is_negative = unscaled_hugeint < 0;
		for (int i = 0; i < huge_int_bytes; i++) {
			uint8_t get_8 =
			    static_cast<uint8_t>(static_cast<uhugeint_t>(unscaled_hugeint >> ((huge_int_bytes - i - 1) * 8)));
			if (is_negative && (get_8 == 0xFF) && !first_val) {
				// number is negative, these are sign-extending bytes that are not important
				continue;
			}
			if (!is_negative && (get_8 == 0x00) && !first_val) {
				// number is positive and these sign-extending bytes that are not important
				continue;
			}
			if (!first_val) {
				// check if we need padding
				if (is_negative && ((get_8 & 0x80) != 0x80)) {
					// negative padding is needed. the number is negative
					// but the most significant byte is not 1
					needs_negative_padding = true;
				} else if (!is_negative && ((get_8 & 0x80) == 0x80)) {
					// yes padding needed, number is positive but most significant byte is 1,
					// sign extend with one byte of 0x00
					needs_positive_padding = true;
				}
				first_val = true;
			}
			big_endian_bytes.push_back(get_8);
		}
		if (!first_val) {
			// value is 0 or -1
			big_endian_bytes.push_back(is_negative ? (uint8_t)0xFF : (uint8_t)0x00);
		} else {
			// insert padding at the beginning so big endian encoding is unambiguous
			if (needs_negative_padding) {
				big_endian_bytes.insert(big_endian_bytes.begin(), 0xFF);
			}
			if (needs_positive_padding) {
				big_endian_bytes.insert(big_endian_bytes.begin(), 0x00);
			}
		}

		// use big_endian_bytes directly as it is already in big endian order
		auto ret_val = Value::BLOB(const_data_ptr_cast<uint8_t>(big_endian_bytes.data()), big_endian_bytes.size());
		auto ret = SerializeResult(column_type, ret_val);
		return ret;
	}
	case LogicalTypeId::BLOB: {
		// do not double serialize blob values.
		if (input_value.type() != LogicalType::VARCHAR) {
			return SerializeResult(column_type, input_value);
		}
		// get const data ptr for the string value
		auto val = input_value.GetValue<string>();
		auto bytes = HexStringToBytes(val);
		auto serialized_const_data_ptr = const_data_ptr_cast<uint8_t>(bytes.data());
		// create blob value of int32
		auto ret_val = Value::BLOB(serialized_const_data_ptr, bytes.size());
		auto ret = SerializeResult(column_type, ret_val);
		return ret;
	}
		// GEOMETRY and VARIANT bounds are not produced via this string-Value path;
		// they require multi-double / variant-binary encodings built inline at the
		// stats-collection site (see IcebergInsertGlobalState::AddFiles).
	case LogicalTypeId::GEOMETRY:
	case LogicalTypeId::VARIANT:
		// boolean does not yet return proper values so we skip
	case LogicalTypeId::BOOLEAN:
	default:
		break;
	}
	// return no serialized value, also no error.
	return SerializeResult();
}

} // namespace duckdb
