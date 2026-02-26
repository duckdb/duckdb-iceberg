#include "iceberg_hash.hpp"
#include "duckdb/common/exception.hpp"

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

//! Hash int32 value
//! Iceberg spec: BucketUtil.hash(int) calls Guava's MURMUR3.hashLong(value),
//! which sign-extends the int to a long (8 bytes) before hashing.
//! So we must hash the int as a sign-extended int64 (8 bytes), not 4 bytes.
int32_t IcebergHash::HashInt32(int32_t value) {
	// Sign-extend int32 to int64, then hash as 8-byte little-endian
	return HashInt64(static_cast<int64_t>(value));
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

//! Hash date (Iceberg spec: days since epoch as int32, hashed as int64)
int32_t IcebergHash::HashDate(date_t date) {
	return HashInt32(date.days);
}

//! Hash a DuckDB Value based on its type
//! Supports the most common Iceberg bucket transform types:
//! integer, long, date, string
int32_t IcebergHash::HashValue(const Value &value) {
	if (value.IsNull()) {
		// Iceberg spec: null values hash to 0
		return 0;
	}

	switch (value.type().id()) {
	// integer / date: hashed as int64 (sign-extended), matching BucketUtil.hash(int)
	case LogicalTypeId::INTEGER:
		return HashInt32(value.GetValue<int32_t>());
	case LogicalTypeId::DATE:
		return HashDate(value.GetValue<date_t>());
	// long: hashed as int64
	case LogicalTypeId::BIGINT:
		return HashInt64(value.GetValue<int64_t>());
	// string: hashed as UTF-8 bytes
	case LogicalTypeId::VARCHAR: {
		auto str_val = value.ToString();
		return HashString(string_t(str_val));
	}

	// Unsupported types - return 0 as safe default
	default:
		return 0;
	}
}

} // namespace duckdb
