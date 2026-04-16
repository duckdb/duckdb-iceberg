//===----------------------------------------------------------------------===//
//                         DuckDB
//
// core/expression/iceberg_hash.hpp
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
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/hugeint.hpp"

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
	static int32_t HashDate(date_t date);
	static int32_t HashDecimal(const Value &value);
	//! Hash raw unscaled decimal values (for vector execution without boxing to Value)
	static int32_t HashDecimalInt64(int64_t unscaled);
	static int32_t HashDecimalHugeInt(hugeint_t unscaled);
	static int32_t HashTime(dtime_t t);
	static int32_t HashTimestampNs(timestamp_ns_t t);
	static int32_t HashUUID(hugeint_t uuid);

	//! Hash a DuckDB Value based on its type
	static int32_t HashValue(const Value &value);

	//! High-level Iceberg transform helpers (shared by ApplyTransform and scalar functions)
	//! BucketValue: returns Value::INTEGER(bucket_id), or null Value for null/unsupported input
	static Value BucketValue(const Value &v, int32_t num_buckets);
	//! TruncateValue: returns the truncated Value preserving type; throws for unsupported types
	static Value TruncateValue(const Value &v, idx_t width);

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
