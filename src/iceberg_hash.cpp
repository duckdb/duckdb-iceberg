#include "iceberg_hash.hpp"
#include "duckdb/common/exception.hpp"
#include <cstring>

namespace duckdb {

//! MurmurHash3 32-bit implementation
//! This is the standard implementation matching Apache Iceberg specification
int32_t IcebergHash::Murmur3Hash32(const uint8_t *data, idx_t len, uint32_t seed) {
	uint32_t h1 = seed;
	const idx_t nblocks = len / 4;

	// Body: process 4-byte blocks
	const uint32_t *blocks = reinterpret_cast<const uint32_t *>(data);
	for (idx_t i = 0; i < nblocks; i++) {
		uint32_t k1 = blocks[i];

		k1 *= C1;
		k1 = RotateLeft(k1, 15);
		k1 *= C2;

		h1 ^= k1;
		h1 = RotateLeft(h1, 13);
		h1 = h1 * 5 + 0xe6546b64;
	}

	// Tail: process remaining bytes
	const uint8_t *tail = data + nblocks * 4;
	uint32_t k1 = 0;

	switch (len & 3) {
	case 3:
		k1 ^= tail[2] << 16;
		// fallthrough
	case 2:
		k1 ^= tail[1] << 8;
		// fallthrough
	case 1:
		k1 ^= tail[0];
		k1 *= C1;
		k1 = RotateLeft(k1, 15);
		k1 *= C2;
		h1 ^= k1;
	}

	// Finalization
	h1 ^= len;
	h1 = FMix32(h1);

	return static_cast<int32_t>(h1);
}

//! Hash int32 value (Iceberg spec: hash as 4-byte little-endian)
int32_t IcebergHash::HashInt32(int32_t value) {
	uint8_t bytes[4];
	// Little-endian encoding
	bytes[0] = static_cast<uint8_t>(value);
	bytes[1] = static_cast<uint8_t>(value >> 8);
	bytes[2] = static_cast<uint8_t>(value >> 16);
	bytes[3] = static_cast<uint8_t>(value >> 24);
	return Murmur3Hash32(bytes, 4, SEED);
}

//! Hash int64 value (Iceberg spec: hash as 8-byte little-endian)
int32_t IcebergHash::HashInt64(int64_t value) {
	uint8_t bytes[8];
	// Little-endian encoding
	for (int i = 0; i < 8; i++) {
		bytes[i] = static_cast<uint8_t>(value >> (i * 8));
	}
	return Murmur3Hash32(bytes, 8, SEED);
}

//! Hash string value (Iceberg spec: hash UTF-8 bytes)
int32_t IcebergHash::HashString(const string_t &value) {
	return Murmur3Hash32(reinterpret_cast<const uint8_t *>(value.GetData()), value.GetSize(), SEED);
}

//! Hash binary/bytes value
int32_t IcebergHash::HashBytes(const uint8_t *data, idx_t len) {
	return Murmur3Hash32(data, len, SEED);
}

//! Hash UUID (Iceberg spec: hash as 16-byte big-endian)
int32_t IcebergHash::HashUUID(const hugeint_t &uuid) {
	uint8_t bytes[16];
	// UUID is stored as two 64-bit integers (upper and lower)
	// Iceberg uses big-endian for UUID
	uint64_t upper = static_cast<uint64_t>(uuid.upper);
	uint64_t lower = static_cast<uint64_t>(uuid.lower);

	// Big-endian encoding
	for (int i = 0; i < 8; i++) {
		bytes[i] = static_cast<uint8_t>(upper >> (56 - i * 8));
		bytes[8 + i] = static_cast<uint8_t>(lower >> (56 - i * 8));
	}
	return Murmur3Hash32(bytes, 16, SEED);
}

//! Hash date (Iceberg spec: days since epoch as int32)
int32_t IcebergHash::HashDate(date_t date) {
	int32_t days = date.days;
	return HashInt32(days);
}

//! Hash timestamp (Iceberg spec: microseconds since epoch as int64)
int32_t IcebergHash::HashTimestamp(timestamp_t timestamp) {
	int64_t micros = timestamp.value;
	return HashInt64(micros);
}

//! Hash timestamp with timezone (same as timestamp)
int32_t IcebergHash::HashTimestampTZ(timestamp_t timestamp) {
	return HashTimestamp(timestamp);
}

//! Hash a DuckDB Value based on its type
int32_t IcebergHash::HashValue(const Value &value) {
	if (value.IsNull()) {
		// Iceberg spec: null values hash to 0
		return 0;
	}

	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		// Iceberg spec: false -> 0, true -> 1, then hash as int32
		return HashInt32(value.GetValue<bool>() ? 1 : 0);
	case LogicalTypeId::TINYINT:
		return HashInt32(value.GetValue<int8_t>());
	case LogicalTypeId::SMALLINT:
		return HashInt32(value.GetValue<int16_t>());
	case LogicalTypeId::INTEGER:
		return HashInt32(value.GetValue<int32_t>());
	case LogicalTypeId::BIGINT:
		return HashInt64(value.GetValue<int64_t>());
	case LogicalTypeId::FLOAT: {
		// Iceberg spec: float as IEEE 754 binary representation
		float f = value.GetValue<float>();
		int32_t bits;
		std::memcpy(&bits, &f, sizeof(float));
		return HashInt32(bits);
	}
	case LogicalTypeId::DOUBLE: {
		// Iceberg spec: double as IEEE 754 binary representation
		double d = value.GetValue<double>();
		int64_t bits;
		std::memcpy(&bits, &d, sizeof(double));
		return HashInt64(bits);
	}
	case LogicalTypeId::VARCHAR: {
		auto str_val = value.ToString();
		return HashString(string_t(str_val));
	}
	case LogicalTypeId::DATE:
		return HashDate(value.GetValue<date_t>());
	case LogicalTypeId::TIMESTAMP:
		return HashTimestamp(value.GetValue<timestamp_t>());
	case LogicalTypeId::TIMESTAMP_TZ:
		return HashTimestampTZ(value.GetValue<timestamp_t>());
	case LogicalTypeId::UUID:
		return HashUUID(value.GetValue<hugeint_t>());
	case LogicalTypeId::BLOB: {
		auto str_val = value.ToString();
		return HashBytes(reinterpret_cast<const uint8_t *>(str_val.c_str()), str_val.length());
	}
	case LogicalTypeId::DECIMAL: {
		// Iceberg spec: decimal as unscaled value (int32, int64, or bytes)
		// DuckDB DECIMAL needs special handling based on width
		auto &decimal_type = value.type();
		auto width = DecimalType::GetWidth(decimal_type);
		auto scale = DecimalType::GetScale(decimal_type);

		// Get unscaled value based on decimal width
		if (width <= 4) {
			// DECIMAL(1-4) stored as int16
			auto unscaled = value.GetValue<int16_t>();
			return HashInt32(static_cast<int32_t>(unscaled));
		} else if (width <= 9) {
			// DECIMAL(5-9) stored as int32
			auto unscaled = value.GetValue<int32_t>();
			return HashInt32(unscaled);
		} else if (width <= 18) {
			// DECIMAL(10-18) stored as int64
			auto unscaled = value.GetValue<int64_t>();
			return HashInt64(unscaled);
		} else {
			// DECIMAL(19-38) stored as hugeint, convert to bytes
			auto unscaled = value.GetValue<hugeint_t>();
			// For large decimals, hash as 16-byte big-endian (similar to UUID)
			uint8_t bytes[16];
			uint64_t upper = static_cast<uint64_t>(unscaled.upper);
			uint64_t lower = static_cast<uint64_t>(unscaled.lower);
			for (int i = 0; i < 8; i++) {
				bytes[i] = static_cast<uint8_t>(upper >> (56 - i * 8));
				bytes[8 + i] = static_cast<uint8_t>(lower >> (56 - i * 8));
			}
			return Murmur3Hash32(bytes, 16, SEED);
		}
	}
	default:
		throw NotImplementedException("Iceberg hash not implemented for type: %s", value.type().ToString());
	}
}

} // namespace duckdb
