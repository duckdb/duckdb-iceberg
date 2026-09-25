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
	//! SQL dialect identifier DuckDB writes/reads for its own Iceberg View representations.
	//! Lowercase matches the convention used by other engines in the Iceberg View Spec (e.g. "spark", "trino").
	static constexpr const char *ViewDuckDBDialect = "duckdb";
	//! Representation `type` value for SQL views per the Iceberg View Spec.
	static constexpr const char *ViewSQLRepresentationType = "sql";
};

} // namespace duckdb
