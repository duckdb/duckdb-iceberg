#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

//! DuckDB has no geography type. A geography field is kept in the schema as a BLOB aliased to its Iceberg type, e.g.
//! `geography(OGC:CRS84, spherical)`, so the rest of the table stays usable and the field can't be read or written.
//! The alias isn't a registered type, so it can't be declared in SQL.
struct IcebergGeographyType {
	static LogicalType Get(const string &iceberg_type);
	static bool IsGeography(const LogicalType &type);
	static bool ContainsGeography(const LogicalType &type);
	static string GetIcebergType(const LogicalType &type);
};

} // namespace duckdb
