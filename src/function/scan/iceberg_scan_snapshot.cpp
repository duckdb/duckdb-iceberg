#include "function/iceberg_scan_snapshot.hpp"

#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "planning/snapshot/iceberg_scan_info.hpp"

namespace duckdb {

optional<IcebergBoundSnapshot> IcebergScanGetSnapshot(optional_ptr<const TableFunctionInfo> function_info) {
	if (!function_info) {
		return nullopt;
	}
	auto scan_info = dynamic_cast<const IcebergScanInfo *>(function_info.get());
	if (!scan_info) {
		return nullopt;
	}
	IcebergBoundSnapshot result;
	result.schema_id = scan_info->snapshot_info.schema_id;
	result.metadata_location = scan_info->metadata_path;
	if (scan_info->snapshot_info.snapshot) {
		result.snapshot_id = scan_info->snapshot_info.snapshot->snapshot_id;
	}
	return result;
}

optional<IcebergBoundSnapshot> IcebergScanGetSnapshot(const TableFunction &function) {
	return IcebergScanGetSnapshot(function.function_info.get());
}

} // namespace duckdb
