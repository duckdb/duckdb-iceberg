#pragma once

#include "metadata/iceberg_column_definition.hpp"
#include "rest_catalog/objects/snapshot.hpp"

namespace duckdb {

struct IcebergTableMetadata;

enum class IcebergSnapshotOperationType : uint8_t { APPEND, REPLACE, OVERWRITE, DELETE };

//! Snapshot summary statistics for Iceberg spec compliance
struct IcebergSnapshotSummary {
	//! Cumulative totals (required by some query engines like Redshift)
	int64_t total_records = 0;
	int64_t total_data_files = 0;
	int64_t total_files_size = 0;
	int64_t total_delete_files = 0;
	int64_t total_position_deletes = 0;
	int64_t total_equality_deletes = 0;

	//! Delta values for this snapshot
	int64_t added_records = 0;
	int64_t added_data_files = 0;
	int64_t added_files_size = 0;
	int64_t deleted_records = 0;
	int64_t deleted_data_files = 0;
	int64_t removed_files_size = 0;
	int64_t changed_partition_count = 0;

	bool has_statistics = false;
};

//! An Iceberg snapshot https://iceberg.apache.org/spec/#snapshots
class IcebergSnapshot {
public:
	IcebergSnapshot() {
	}
	static IcebergSnapshot ParseSnapshot(rest_api_objects::Snapshot &snapshot, IcebergTableMetadata &metadata);
	rest_api_objects::Snapshot ToRESTObject() const;

public:
	//! Snapshot metadata
	int64_t snapshot_id = NumericLimits<int64_t>::Maximum();
	bool has_parent_snapshot = false;
	int64_t parent_snapshot_id = NumericLimits<int64_t>::Maximum();
	int64_t sequence_number;
	int32_t schema_id;
	IcebergSnapshotOperationType operation;
	timestamp_t timestamp_ms;
	string manifest_list;

	//! Summary statistics
	IcebergSnapshotSummary summary;
};

} // namespace duckdb
