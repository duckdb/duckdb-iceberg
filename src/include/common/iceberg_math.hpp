#pragma once

#include "duckdb/common/assert.hpp"

#include <cstdint>

namespace duckdb {

//! mathematical floor of a/b. Works for negative divisors; C++ `/`
//! truncates toward zero, so we adjust when signs differ and there is a remainder.
//! Undefined if b == 0.
inline int64_t IcebergFloorDiv(int64_t a, int64_t b) {
	D_ASSERT(b != 0);
	int64_t res = a / b;
	if ((a ^ b) < 0 && (res * b != a)) {
		res--;
	}
	return res;
}

//! Iceberg DateTimeUtil.nanosToMicros uses Math.floorDiv(nanos, 1000).
inline int64_t IcebergNanosToMicrosFloor(int64_t nanos) {
	constexpr int64_t NANOS_PER_MICRO = 1000;
	return IcebergFloorDiv(nanos, NANOS_PER_MICRO);
}

} // namespace duckdb
