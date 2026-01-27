#pragma once

#include "metadata/iceberg_column_definition.hpp"
#include "metadata/iceberg_snapshot.hpp"
#include "metadata/iceberg_partition_spec.hpp"
#include "metadata/iceberg_sort_order.hpp"
#include "metadata/iceberg_table_schema.hpp"
#include "metadata/iceberg_field_mapping.hpp"

#include "iceberg_options.hpp"
#include "rest_catalog/objects/list.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

// common Iceberg table property keys
const string WRITE_UPDATE_MODE = "write.update.mode";
const string WRITE_DELETE_MODE = "write.delete.mode";

//! A structure to store "LoadTableResult" information that changes as a transaction goes on
//! Everything is parsed from a load table result, but if a transaction changes a schema, those schema
//! updates are reflected here and never within the catalog that lives beyond transactions
struct IcebergTableMetadata {
public:
	IcebergTableMetadata() = default;

public:
	static rest_api_objects::TableMetadata Parse(const string &path, FileSystem &fs,
	                                             const string &metadata_compression_codec);
	static IcebergTableMetadata FromLoadTableResult(const rest_api_objects::LoadTableResult &load_table_result);
	static IcebergTableMetadata FromTableMetadata(const rest_api_objects::TableMetadata &table_metadata);
	static string GetMetaDataPath(ClientContext &context, const string &path, FileSystem &fs,
	                              const IcebergOptions &options);
	optional_ptr<IcebergSnapshot> GetLatestSnapshot();
	const IcebergTableSchema &GetLatestSchema() const;
	bool HasPartitionSpec() const;
	const IcebergPartitionSpec &GetLatestPartitionSpec() const;
	const unordered_map<int32_t, IcebergPartitionSpec> &GetPartitionSpecs() const;

	bool HasSortOrder() const;
	const IcebergSortOrder &GetLatestSortOrder() const;
	const unordered_map<int32_t, IcebergSortOrder> &GetSortOrderSpecs() const;

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
	optional_ptr<const IcebergSortOrder> FindSortOrderById(int32_t sort_id) const;
	optional_ptr<IcebergSnapshot> GetSnapshot(const IcebergSnapshotLookup &lookup);

	//! Get the data and metadata paths, falling back to default if not set
	const string &GetLatestMetadataJson() const;
	const string &GetLocation() const;
	const string GetDataPath() const;
	const string GetMetadataPath() const;

	//! For Nessie catalogs (version ?)
	bool HasLastColumnId() const;
	idx_t GetLastColumnId() const;

	const case_insensitive_map_t<string> &GetTableProperties() const;
	string GetTableProperty(string property_string) const;
	bool PropertiesAllowPositionalDeletes(IcebergSnapshotOperationType operation_type) const;

public:
	string table_uuid;
	// when loading table metadata, store the path to the metadata.json for extension functions like iceberg_metadata()
	string latest_metadata_json;
	string location;

	int32_t iceberg_version;
	int32_t current_schema_id;
	int32_t default_spec_id;
	optional_idx default_sort_order_id;

	bool has_current_snapshot = false;
	int64_t current_snapshot_id;
	int64_t last_sequence_number;
	idx_t last_updated_ms;

	optional_idx last_column_id;

	//! partition_spec_id -> partition spec
	unordered_map<int32_t, IcebergPartitionSpec> partition_specs;
	//! sort_order_id -> sort spec
	unordered_map<int32_t, IcebergSortOrder> sort_specs;
	//! snapshot_id -> snapshot
	unordered_map<int64_t, IcebergSnapshot> snapshots;
	//! schema_id -> schema
	unordered_map<int32_t, shared_ptr<IcebergTableSchema>> schemas;
	vector<IcebergFieldMapping> mappings;

	//! Custom write paths from table properties
	string write_data_path;
	string write_metadata_path;

	//! table properties
	case_insensitive_map_t<string> table_properties;
};

} // namespace duckdb
