#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/value.hpp"

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
	return IcebergFloorDiv(nanos, Interval::NANOS_PER_MICRO);
}

//! Steps a value by one unit of its type: 1 for integers, a day for DATE, a microsecond for TIMESTAMP,
//! a nanosecond for the _NS variants. False when the type has no successor or the step leaves its range.
bool IcebergTryIncrement(const Value &value, Value &result);
bool IcebergTryDecrement(const Value &value, Value &result);

} // namespace duckdb
