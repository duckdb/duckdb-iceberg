#pragma once

#include "metadata/iceberg_column_definition.hpp"
#include "rest_catalog/objects/snapshot.hpp"

namespace duckdb {

struct IcebergTableMetadata;

enum class IcebergSnapshotOperationType : uint8_t { APPEND, REPLACE, OVERWRITE, DELETE };

enum class SnapshotMetricType : uint8_t {
	ADDED_DATA_FILES,
	ADDED_RECORDS,
	DELETED_DATA_FILES,
	DELETED_RECORDS,
	TOTAL_DATA_FILES,
	TOTAL_RECORDS
};

//! An Iceberg snapshot https://iceberg.apache.org/spec/#snapshots
class IcebergSnapshot {
public:
	IcebergSnapshot() {
	}
	static IcebergSnapshot ParseSnapshot(rest_api_objects::Snapshot &snapshot, IcebergTableMetadata &metadata);
	rest_api_objects::Snapshot ToRESTObject() const;
	using metrics_map_t = std::map<SnapshotMetricType, int64_t>;

public:
	//! Snapshot metadata
	int64_t snapshot_id = NumericLimits<int64_t>::Maximum();
	bool has_parent_snapshot = false;
	int64_t parent_snapshot_id = NumericLimits<int64_t>::Maximum();
	int64_t sequence_number;
	int32_t schema_id;
	IcebergSnapshotOperationType operation = IcebergSnapshotOperationType::APPEND;
	timestamp_t timestamp_ms;
	string manifest_list;
	metrics_map_t metrics;
};

} // namespace duckdb
