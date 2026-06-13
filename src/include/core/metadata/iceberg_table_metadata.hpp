#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/storage/caching_file_system_wrapper.hpp"

#include "core/metadata/schema/iceberg_column_definition.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/schema/iceberg_field_mapping.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "core/metadata/sort/iceberg_sort_order.hpp"
#include "iceberg_options.hpp"
#include "rest_catalog/objects/list.hpp"
#include "planning/snapshot/iceberg_snapshot_scan_info.hpp"

namespace duckdb {

// common Iceberg table property keys
const string WRITE_UPDATE_MODE = "write.update.mode";
const string WRITE_DELETE_MODE = "write.delete.mode";

struct IcebergMetadataLogItem {
public:
	IcebergMetadataLogItem(const string &path, int64_t timestamp_ms) : metadata_file(path), timestamp_ms(timestamp_ms) {
	}

public:
	string metadata_file;
	int64_t timestamp_ms;
};

//! A structure to store "LoadTableResult" information that changes as a transaction goes on
//! Everything is parsed from a load table result, but if a transaction changes a schema, those schema
//! updates are reflected here and never within the catalog that lives beyond transactions
struct IcebergTableMetadata {
public:
	IcebergTableMetadata(const IcebergTableMetadata &) = delete;
	IcebergTableMetadata &operator=(const IcebergTableMetadata &) = delete;
	IcebergTableMetadata(IcebergTableMetadata &&) = default;
	IcebergTableMetadata &operator=(IcebergTableMetadata &&) = default;

public:
	static rest_api_objects::TableMetadata Parse(const string &path, FileSystem &fs,
	                                             const string &metadata_compression_codec);
	static IcebergTableMetadata FromTableMetadata(const rest_api_objects::TableMetadata &table_metadata);
	static string GetMetaDataPath(ClientContext &context, const string &path, FileSystem &fs,
	                              const IcebergOptions &options);
	optional_ptr<const IcebergSnapshot> GetLatestSnapshot() const;
	const IcebergTableSchema &GetLatestSchema() const;
	bool HasPartitionSpec() const;
	const IcebergPartitionSpec &GetLatestPartitionSpec() const;
	const unordered_map<int32_t, IcebergPartitionSpec> &GetPartitionSpecs() const;

	bool HasSortOrder() const;
	const IcebergSortOrder &GetLatestSortOrder() const;
	const unordered_map<int32_t, IcebergSortOrder> &GetSortOrderSpecs() const;

	optional_ptr<const IcebergSnapshot> GetSnapshotById(int64_t snapshot_id) const;
	optional_ptr<const IcebergSnapshot> GetSnapshotByTimestamp(timestamp_t timestamp) const;

	//! Whether `descendant_id` is `ancestor_id`, or reachable from it by following parent_snapshot_id
	//! links. Used by the retry-time guard to confirm the refreshed parent still descends from the
	//! snapshot this transaction started against (a concurrent rollback would break this). If a link
	//! in the chain is missing from the loaded `snapshots` map (e.g. the catalog elided an expired
	//! snapshot), the walk stops and returns false -- callers treat "cannot prove ancestry" as a
	//! conflict, which is the safe choice.
	bool IsAncestorOf(int64_t ancestor_id, int64_t descendant_id) const;

	//! Version extraction and identification
	static bool UnsafeVersionGuessingEnabled(ClientContext &context);
	static string GetTableVersionFromHint(const string &path, FileSystem &fs, string version_format);
	static string GuessTableVersion(const string &meta_path, FileSystem &fs, const IcebergOptions &options);
	static string PickTableVersion(vector<OpenFileInfo> &found_metadata, string &version_pattern, string &glob);

	//! Internal JSON parsing functions
	optional_ptr<const IcebergSnapshot> FindSnapshotByIdInternal(int64_t target_id) const;
	shared_ptr<IcebergTableSchema> GetSchemaFromId(int32_t schema_id) const;
	//! Searches every schema (current and historical) for a top-level column with the given field-id.
	//! Used to resolve equality-delete columns that have since been dropped from the table.
	optional_ptr<const IcebergColumnDefinition> FindColumnByFieldId(int32_t field_id) const;
	optional_ptr<const IcebergPartitionSpec> FindPartitionSpecById(int32_t spec_id) const;
	optional_ptr<const IcebergSortOrder> FindSortOrderById(int32_t sort_id) const;
	IcebergSnapshotScanInfo GetSnapshot(const IcebergSnapshotLookup &lookup) const;

	const string &GetLocation() const;
	const string GetDataPath(FileSystem &fs) const;
	const string GetMetadataPath(FileSystem &fs) const;

	bool HasLastColumnId() const;
	idx_t GetLastColumnId() const;

	bool HasLastPartitionId() const;
	int32_t GetLastPartitionFieldId() const;

	const case_insensitive_map_t<string> &GetTableProperties() const;
	string GetTableProperty(string property_string) const;
	bool PropertiesAllowPositionalDeletes(IcebergSnapshotOperationType operation_type) const;
	string ToJSON() const;
	void WriteMetadata(ClientContext &context, const string &path) const;
	void WriteVersionHint(ClientContext &context, const string &path, const string &metadata_json_path) const;

public:
	void SetCurrentSchemaId(int32_t schema_id);
	int32_t GetCurrentSchemaId() const;

	IcebergTableSchema &AddSchemaOrGetExisting(shared_ptr<IcebergTableSchema> schema);
	const unordered_map<int32_t, shared_ptr<IcebergTableSchema>> &GetSchemas() const;

private:
	yyjson_mut_val *SchemasToJSON(yyjson_mut_doc *doc) const;
	yyjson_mut_val *PartitionsToJSON(yyjson_mut_doc *doc) const;
	yyjson_mut_val *TablePropertiesToJSON(yyjson_mut_doc *doc) const;
	yyjson_mut_val *SnapshotsToJSON(yyjson_mut_doc *doc) const;
	yyjson_mut_val *SnapshotLogToJSON(yyjson_mut_doc *doc) const;
	yyjson_mut_val *SortOrdersToJSON(yyjson_mut_doc *doc) const;

public:
	string table_uuid;
	string location;

	int32_t iceberg_version;
	int32_t default_spec_id;
	bool has_next_row_id = false;
	int64_t next_row_id = 0xDEADBEEF;
	optional_idx default_sort_order_id;

	bool has_current_snapshot = false;
	int64_t current_snapshot_id;
	int64_t last_sequence_number;
	timestamp_t last_updated_ms;

	optional_idx last_column_id;
	optional_idx last_partition_field_id;

	//! partition_spec_id -> partition spec
	unordered_map<int32_t, IcebergPartitionSpec> partition_specs;
	//! sort_order_id -> sort spec
	unordered_map<int32_t, IcebergSortOrder> sort_specs;
	//! snapshot_id -> snapshot
	unordered_map<int64_t, IcebergSnapshot> snapshots;
	//! History of refs.main per Iceberg spec "snapshot-log": (snapshot_id, raw millis)
	//! pairs recording each change to current-snapshot-id, sorted ascending by ms.
	//! Used for spec-compliant point-in-time resolution; side-branch commits are absent.
	//! Stored as raw millis (matching the REST API representation and last_updated_ms)
	//! to keep all timestamp comparisons in a single unit.
	vector<pair<int64_t /*snapshot_id*/, int64_t /*timestamp_ms*/>> snapshot_log;
	vector<IcebergFieldMapping> mappings;

	//! Custom write paths from table properties
	string write_data_path;
	string write_metadata_path;

	//! table properties
	case_insensitive_map_t<string> table_properties;

	vector<IcebergMetadataLogItem> metadata_log;

public:
	IcebergTableMetadata() = default;

private:
	int32_t current_schema_id;
	//! schema_id -> schema
	unordered_map<int32_t, shared_ptr<IcebergTableSchema>> schemas;
};

} // namespace duckdb
