#pragma once

#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "core/metadata/snapshot/iceberg_snapshot_metrics.hpp"
#include "rest_catalog/objects/snapshot.hpp"

namespace duckdb {

struct IcebergTableMetadata;
struct IcebergTable;

enum class IcebergSnapshotOperationType : uint8_t { APPEND, REPLACE, OVERWRITE, DELETE };

//! An Iceberg snapshot https://iceberg.apache.org/spec/#snapshots
class IcebergSnapshot {
public:
	IcebergSnapshot(int32_t schema_id) : schema_id(schema_id) {
	}
	static int64_t NewSnapshotId();
	static IcebergSnapshot ParseSnapshot(const rest_api_objects::Snapshot &snapshot, IcebergTableMetadata &metadata);
	rest_api_objects::Snapshot ToRESTObject(const IcebergTableMetadata &table_metadata) const;

public:
	int32_t GetSchemaId() const;

private:
	int32_t schema_id;

public:
	//! Snapshot metadata
	optional<int64_t> snapshot_id;
	optional<int64_t> parent_snapshot_id;
	optional<int64_t> sequence_number;
	optional<int64_t> first_row_id;
	optional<int64_t> added_rows;
	IcebergSnapshotOperationType operation;
	timestamp_ms_t timestamp_ms;
	string manifest_list;
	//! V1 snapshots may embed manifest file paths instead of referencing a manifest list.
	vector<string> manifests;
	IcebergSnapshotMetrics metrics;
};

} // namespace duckdb
