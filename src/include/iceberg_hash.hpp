//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_hash.hpp
//
// Iceberg-compatible hashing functions for bucket partitioning
// Based on Apache Iceberg specification:
// https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

class IcebergHash {
public:
	//! MurmurHash3 32-bit implementation
	//! Based on: https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
	static int32_t Murmur3Hash32(const uint8_t *data, idx_t len, uint32_t seed = 0);

	//! Hash functions for different types (Iceberg spec compliant)
	static int32_t HashInt32(int32_t value);
	static int32_t HashInt64(int64_t value);
	static int32_t HashString(const string_t &value);
	static int32_t HashBytes(const uint8_t *data, idx_t len);
	static int32_t HashUUID(const hugeint_t &uuid);
	static int32_t HashDate(date_t date);
	static int32_t HashTimestamp(timestamp_t timestamp);
	static int32_t HashTimestampTZ(timestamp_t timestamp);

	//! Hash a DuckDB Value based on its type
	static int32_t HashValue(const Value &value);

private:
	static constexpr uint32_t C1 = 0xcc9e2d51;
	static constexpr uint32_t C2 = 0x1b873593;
	static constexpr uint32_t SEED = 0;

	static inline uint32_t RotateLeft(uint32_t x, uint8_t r) {
		return (x << r) | (x >> (32 - r));
	}

	static inline uint32_t FMix32(uint32_t h) {
		h ^= h >> 16;
		h *= 0x85ebca6b;
		h ^= h >> 13;
		h *= 0xc2b2ae35;
		h ^= h >> 16;
		return h;
	}
};

} // namespace duckdb
