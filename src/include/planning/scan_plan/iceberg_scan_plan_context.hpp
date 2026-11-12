#pragma once

#include "planning/snapshot/iceberg_scan_info.hpp"
#include "iceberg_options.hpp"

namespace duckdb {

struct IcebergDeleteFileReference {
	idx_t manifest_idx;
	idx_t entry_idx;
};

struct IcebergScanPlanContext {
	ClientContext &context;
	FileSystem &fs;
	const string &path;
	const IcebergOptions &options;
	const IcebergSnapshotScanInfo &snapshot;
	const IcebergTableMetadata &metadata;
	const IcebergTableSchema &schema;
	optional_ptr<const IcebergTransactionData> transaction_data;
};

} // namespace duckdb
