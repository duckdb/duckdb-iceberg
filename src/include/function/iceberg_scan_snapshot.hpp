//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/iceberg_scan_snapshot.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! The snapshot an iceberg_scan was bound to, resolved from the table's version
//! hint, an AT clause, or a snapshot option.  A consumer of a bound
//! iceberg_scan -- plan serialisation, tooling, planning the same scan with
//! iceberg_scan_plan -- otherwise has no way to learn which snapshot the scan
//! reads.
struct IcebergBoundSnapshot {
	//! The snapshot id, or unset for a table with no snapshot (an empty table).
	optional<int64_t> snapshot_id;
	//! The schema id the scan reads with.
	int32_t schema_id;
	//! The table's metadata location.
	string metadata_location;
};

//! The bound snapshot of an iceberg_scan, from the table function's info, or
//! unset when `function` is not an iceberg_scan.  Depends only on core DuckDB
//! types, so a caller needs no Iceberg headers.
optional<IcebergBoundSnapshot> IcebergScanGetSnapshot(const TableFunction &function);
optional<IcebergBoundSnapshot> IcebergScanGetSnapshot(optional_ptr<const TableFunctionInfo> function_info);

} // namespace duckdb
