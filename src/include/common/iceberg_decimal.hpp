//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/iceberg_decimal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

//! Limits of the Iceberg decimal type. These come from the Iceberg spec, not from DuckDB:
//! DuckDB's own DECIMAL can be wider than what an Iceberg table can store.
struct IcebergDecimal {
	//! SPEC: decimal(P, S) - "Precision must be 38 or less"
	static constexpr const idx_t MAX_PRECISION = 38;

	//! Whether 'type' is a DECIMAL that Iceberg can represent
	static bool IsSupported(const LogicalType &type);
	//! Throws if 'type' is a DECIMAL that is too wide for Iceberg
	static void Verify(const LogicalType &type);
};

} // namespace duckdb
