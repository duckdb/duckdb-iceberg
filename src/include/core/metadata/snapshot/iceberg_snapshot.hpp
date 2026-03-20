#pragma once

#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "rest_catalog/objects/snapshot.hpp"

namespace duckdb {

struct IcebergTableMetadata;
struct IcebergTableInformation;

enum class IcebergSnapshotOperationType : uint8_t { APPEND, REPLACE, OVERWRITE, DELETE };

enum class IcebergSnapshotMetricType : uint8_t {
	ADDED_DATA_FILES,
	ADDED_RECORDS,
	DELETED_DATA_FILES,
	DELETED_RECORDS,
	TOTAL_DATA_FILES,
	TOTAL_RECORDS
};

class IcebergSnapshot;
struct IcebergManifestFile;

struct IcebergSnapshotMetrics {
public:
	IcebergSnapshotMetrics();
	IcebergSnapshotMetrics(const IcebergSnapshot &parent_snapshot);

public:
	void AddManifestFile(const IcebergManifestFile &manifest_file);

public:
	map<IcebergSnapshotMetricType, int64_t> metrics;
};

//! An Iceberg snapshot https://iceberg.apache.org/spec/#snapshots
class IcebergSnapshot {
public:
	IcebergSnapshot() {
	}
	static int64_t NewSnapshotId();
	static IcebergSnapshot ParseSnapshot(const rest_api_objects::Snapshot &snapshot, IcebergTableMetadata &metadata);
	rest_api_objects::Snapshot ToRESTObject(const IcebergTableMetadata &table_metadata) const;
	static yyjson_mut_val *ToJSON(const rest_api_objects::Snapshot &snapshot, yyjson_mut_doc *doc);

public:
	//! Snapshot metadata
	int64_t snapshot_id = NumericLimits<int64_t>::Maximum();
	bool has_parent_snapshot = false;
	int64_t parent_snapshot_id = NumericLimits<int64_t>::Maximum();
	int64_t sequence_number = 0xDEADBEEF;
	int32_t schema_id;
	bool has_first_row_id = false;
	int64_t first_row_id = 0xDEADBEEF;
	bool has_added_rows = false;
	int64_t added_rows = 0;
	IcebergSnapshotOperationType operation;
	timestamp_t timestamp_ms;
	string manifest_list;
	IcebergSnapshotMetrics metrics;
};

} // namespace duckdb
