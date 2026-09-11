#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

struct IcebergTableMetadata;

struct ExpireSnapshotsPlanInput {
	QualifiedName table_name;
	optional<timestamp_t> older_than;
	optional<int64_t> retain_last;
	vector<int64_t> snapshot_ids;
};

struct ExpireSnapshotsPlan {
	vector<int64_t> to_expire;
	int64_t retained_snapshots = 0;
};

//! Select snapshots to remove without mutating table metadata.
ExpireSnapshotsPlan PlanSnapshotExpiration(const IcebergTableMetadata &metadata, const ExpireSnapshotsPlanInput &input,
                                           timestamp_ms_t now);

} // namespace duckdb
