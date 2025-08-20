#pragma once

#include "metadata/iceberg_column_definition.hpp"
#include "metadata/iceberg_snapshot.hpp"
#include "metadata/iceberg_partition_spec.hpp"
#include "metadata/iceberg_table_schema.hpp"
#include "metadata/iceberg_field_mapping.hpp"

#include "iceberg_options.hpp"
#include "rest_catalog/objects/table_metadata.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

struct IcebergTableMetadata {
public:
	IcebergTableMetadata() = default;

public:
	static rest_api_objects::TableMetadata Parse(const string &path, FileSystem &fs,
	                                             const string &metadata_compression_codec);
	static IcebergTableMetadata FromTableMetadata(rest_api_objects::TableMetadata &table_metadata);
	static string GetMetaDataPath(ClientContext &context, const string &path, FileSystem &fs,
	                              const IcebergOptions &options);
	optional_ptr<IcebergSnapshot> GetLatestSnapshot();
	const IcebergTableSchema &GetLatestSchema() const;
	const IcebergPartitionSpec &GetLatestPartitionSpec() const;
	optional_ptr<IcebergSnapshot> GetSnapshotById(int64_t snapshot_id);
	optional_ptr<IcebergSnapshot> GetSnapshotByTimestamp(timestamp_t timestamp);

	//! Version extraction and identification
	static bool UnsafeVersionGuessingEnabled(ClientContext &context);
	static string GetTableVersionFromHint(const string &path, FileSystem &fs, string version_format);
	static string GuessTableVersion(const string &meta_path, FileSystem &fs, const IcebergOptions &options);
	static string PickTableVersion(vector<OpenFileInfo> &found_metadata, string &version_pattern, string &glob);

	//! Internal JSON parsing functions
	optional_ptr<IcebergSnapshot> FindSnapshotByIdInternal(int64_t target_id);
	optional_ptr<IcebergSnapshot> FindSnapshotByIdTimestampInternal(timestamp_t timestamp);
	shared_ptr<IcebergTableSchema> GetSchemaFromId(int32_t schema_id) const;
	optional_ptr<const IcebergPartitionSpec> FindPartitionSpecById(int32_t spec_id) const;
	optional_ptr<IcebergSnapshot> GetSnapshot(const IcebergSnapshotLookup &lookup);

public:
	string table_uuid;
	string location;

	int32_t iceberg_version;
	int32_t current_schema_id;
	int32_t default_spec_id;

	bool has_current_snapshot = false;
	int64_t current_snapshot_id;
	int64_t last_sequence_number;

	//! partition_spec_id -> partition spec
	unordered_map<int32_t, IcebergPartitionSpec> partition_specs;
	//! snapshot_id -> snapshot
	unordered_map<int64_t, IcebergSnapshot> snapshots;
	//! schema_id -> schema
	unordered_map<int32_t, shared_ptr<IcebergTableSchema>> schemas;
	vector<IcebergFieldMapping> mappings;
};

} // namespace duckdb
