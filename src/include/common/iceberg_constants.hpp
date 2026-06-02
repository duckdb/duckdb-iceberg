//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/iceberg_constants.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

// store any constant Iceberg strings
class IcebergConstants {
public:
	static constexpr const char *DefaultGeometryCRS = "OGC:CRS84";
};

} // namespace duckdb
