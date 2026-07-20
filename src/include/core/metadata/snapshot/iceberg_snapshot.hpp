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
	ADDED_DELETE_FILES,
	ADDED_POSITION_DELETES,
	REMOVED_DELETE_FILES,
	REMOVED_POSITION_DELETE_FILES,
	ADDED_FILES_SIZE,
	REMOVED_FILES_SIZE,
	TOTAL_DATA_FILES,
	TOTAL_RECORDS,
	TOTAL_DELETE_FILES,
	TOTAL_POSITION_DELETES,
	TOTAL_FILES_SIZE
};

class IcebergSnapshot;
struct IcebergManifestListEntry;

struct IcebergSnapshotMetrics {
public:
	IcebergSnapshotMetrics();
	IcebergSnapshotMetrics(const IcebergSnapshot &parent_snapshot);

public:
	void AddManifestListEntry(const IcebergManifestListEntry &manifest_list_entry);
	void RemoveFileSize(int64_t file_size_in_bytes);
	bool HasTotalFilesSize() const;
	void SetTotalFilesSize(int64_t total_files_size);

private:
	void AddSizeMetric(IcebergSnapshotMetricType type, int64_t value);
	void UpdateTotalFilesSize(int64_t added, int64_t removed);

public:
	map<IcebergSnapshotMetricType, int64_t> metrics;
};

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
	IcebergSnapshotMetrics metrics;
};

} // namespace duckdb
