#include "iceberg_value.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/bswap.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "mbedtls/ecp.h"
#include "iostream"

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
	hugeint_t ret;

	//! The blob has to be smaller or equal to the size of the type
	D_ASSERT(blob.GetSize() <= sizeof(hugeint_t));

	// Convert from big-endian to host byte order
	const uint8_t *src = reinterpret_cast<const uint8_t *>(blob.GetData());
	int64_t upper_val = 0;
	uint64_t lower_val = 0;

	// Calculate how many bytes go into upper and lower parts
	idx_t upper_bytes = (blob.GetSize() <= sizeof(uint64_t)) ? blob.GetSize() : (blob.GetSize() - sizeof(uint64_t));

	// Read upper part (big-endian)
	for (idx_t i = 0; i < upper_bytes; i++) {
		upper_val = (upper_val << 8) | src[i];
	}

	// Handle sign extension for negative numbers
	if (blob.GetSize() > 0 && (src[0] & 0x80)) {
		// Fill remaining bytes with 1s for negative numbers
		if (upper_bytes < sizeof(int64_t)) {
			// Create a mask with 1s in the upper bits that need to be filled
			int64_t mask = ((int64_t)1 << ((sizeof(int64_t) - upper_bytes) * 8)) - 1;
			mask = mask << (upper_bytes * 8);
			upper_val |= mask;
		}
	}

	// Read lower part if there are remaining bytes
	if (blob.GetSize() > sizeof(int64_t)) {
		for (idx_t i = upper_bytes; i < blob.GetSize(); i++) {
			lower_val = (lower_val << 8) | src[i];
		}
	}

	ret = hugeint_t(upper_val, lower_val);
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
		std::memcpy(&val.micros, blob.GetData(), sizeof(int64_t));
		return Value::TIME(val);
	}
	case LogicalTypeId::TIMESTAMP_NS:
		//! FIXME: When support for 'TIMESTAMP_NS' is added,
		//! keep in mind that the value should be inferred as DATE when the blob size is 4

		//! TIMESTAMP_NS is added as part of Iceberg V3
		return DeserializeError(blob, type);
	case LogicalTypeId::UUID:
		return DeserializeUUID(blob, type);
	// Add more types as needed
	default:
		break;
	}
	return DeserializeError(blob, type);
}

std::string truncate_and_increment_utf8(const std::string &input) {
	std::vector<unsigned char> bytes(input.begin(), input.end());
	// Truncate to first 16 bytes
	idx_t n = std::min<size_t>(16, bytes.size());
	if (n == 0) {
		return std::string(bytes.begin(), bytes.end());
	}
	bytes.resize(n);
	idx_t i = n - 1;
	while (((bytes[i] & 0xC0) == 0x80) && i > 0) {
		// skip continuation bytes
		--i;
	}
	bytes[i]++;

	// Convert back to string
	return std::string(bytes.begin(), bytes.end());
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

SerializeResult IcebergValue::SerializeValue(Value input_value, LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::INTEGER: {
		// get const data ptr for the string value
		int32_t val = input_value.GetValue<int32_t>();
		// get const data_ptr of the int32
		auto serialized_const_data_ptr = const_data_ptr_cast<int32_t>(&val);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(int32_t));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::BIGINT: {
		// get const data ptr for the string value
		int64_t val = input_value.GetValue<int64_t>();
		// get const data_ptr of the int32
		auto serialized_const_data_ptr = const_data_ptr_cast<int64_t>(&val);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(int64_t));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::VARCHAR: {
		// get const data ptr for the string value
		string val = truncate_and_increment_utf8(input_value.GetValue<string>());
		// create blob value of int32
		auto serialized_val = val;
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::FLOAT: {
		// get const data ptr for the string value
		float val = input_value.GetValue<float>();
		// get const data_ptr of the int32
		auto serialized_const_data_ptr = const_data_ptr_cast<float>(&val);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(float));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::DOUBLE: {
		// get const data ptr for the string value
		double val = input_value.GetValue<double>();
		// get const data_ptr of the int32
		auto serialized_const_data_ptr = const_data_ptr_cast<double>(&val);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(double));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::DATE: {
		// get const data ptr for the string value
		date_t val = input_value.GetValue<date_t>();
		int32_t epoch_days = Date::EpochDays(val);
		// get const data_ptr of the int32
		auto serialized_const_data_ptr = const_data_ptr_cast<int32_t>(&epoch_days);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(int32_t));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::TIMESTAMP: {
		// get const data ptr for the string value
		timestamp_t val = input_value.GetValue<timestamp_t>();
		int64_t micros_since_epoch = Timestamp::GetEpochMicroSeconds(val);
		// get const data_ptr of the int32
		auto serialized_const_data_ptr = const_data_ptr_cast<int64_t>(&micros_since_epoch);
		// create blob value of int32
		auto serialized_val = Value::BLOB(serialized_const_data_ptr, sizeof(int64_t));
		auto ret = SerializeResult(column_type, serialized_val);
		return ret;
	}
	case LogicalTypeId::DECIMAL: {
		auto decimal_as_string = input_value.GetValue<string>();
		auto dec_pos = decimal_as_string.find(".");
		// remove the decimal point
		decimal_as_string.erase(dec_pos, 1);
		auto unscaled = Value(decimal_as_string).DefaultCastAs(LogicalType::HUGEINT);
		auto unscaled_hugeint = unscaled.GetValue<hugeint_t>();
		vector<uint8_t> big_endian_bytes;
		bool needs_positive_padding = false;
		bool needs_negative_padding = false;
		auto huge_int_bytes = sizeof(hugeint_t);
		bool first_val = false;
		bool is_negative = unscaled_hugeint < 0;
		for (int i = 0; i < huge_int_bytes; i++) {
			uint8_t get_8 = static_cast<uint8_t>(
			    static_cast<uhugeint_t>(unscaled_hugeint >> ((huge_int_bytes - i - 1) * 8)));
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
					// but the most significatn byte is not 1
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
		if (needs_negative_padding) {
			big_endian_bytes.push_back(0xFF);
		}
		if (needs_positive_padding) {
			big_endian_bytes.push_back(0x00);
		}

		// reverse the bytes to get them in big-endian order
		std::reverse(big_endian_bytes.begin(), big_endian_bytes.end());

		hugeint_t result = 0;
		int n = big_endian_bytes.size();
		D_ASSERT(n <= 16);
		for (int i = 0; i < n; i++) {
			result |= static_cast<hugeint_t>(big_endian_bytes[i]) << (8 * (n - i - 1));
		}

		auto serialized_const_data_ptr = const_data_ptr_cast<hugeint_t>(&result);
		// create blob value of int32, using only big_endian_bytes.size() bytes
		auto ret_val = Value::BLOB(serialized_const_data_ptr, big_endian_bytes.size());
		auto ret = SerializeResult(column_type, ret_val);
		return ret;
	}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT: {
		// get const data ptr for the string value
		auto val = input_value.GetValue<string>();
		auto bytes = HexStringToBytes(val);
		// get const data_ptr of the int32
		auto serialized_const_data_ptr = const_data_ptr_cast<uint8_t>(bytes.data());
		// create blob value of int32
		auto ret_val = Value::BLOB(serialized_const_data_ptr, bytes.size());
		auto ret = SerializeResult(column_type, ret_val);
		return ret;
	}
	case LogicalTypeId::BOOLEAN: {
		return {column_type, Value()};
	}
	default:
		break;
	}
	string error = "Invalid column type";
	return SerializeResult(error);
}

} // namespace duckdb
