
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/encrypted_key.hpp"
#include "rest_catalog/objects/metadata_log.hpp"
#include "rest_catalog/objects/partition_spec.hpp"
#include "rest_catalog/objects/partition_statistics_file.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/snapshot.hpp"
#include "rest_catalog/objects/snapshot_log.hpp"
#include "rest_catalog/objects/snapshot_references.hpp"
#include "rest_catalog/objects/sort_order.hpp"
#include "rest_catalog/objects/statistics_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TableMetadata {
public:
	TableMetadata();
	TableMetadata(const TableMetadata &) = delete;
	TableMetadata &operator=(const TableMetadata &) = delete;
	TableMetadata(TableMetadata &&) = default;
	TableMetadata &operator=(TableMetadata &&) = default;

public:
	static TableMetadata FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int32_t format_version;
	string table_uuid;
	string location;
	bool has_location = false;
	int64_t last_updated_ms;
	bool has_last_updated_ms = false;
	int64_t next_row_id;
	bool has_next_row_id = false;
	case_insensitive_map_t<string> properties;
	bool has_properties = false;
	vector<Schema> schemas;
	bool has_schemas = false;
	int32_t current_schema_id;
	bool has_current_schema_id = false;
	int32_t last_column_id;
	bool has_last_column_id = false;
	vector<PartitionSpec> partition_specs;
	bool has_partition_specs = false;
	int32_t default_spec_id;
	bool has_default_spec_id = false;
	// in the IRC spec, it is last_partition_id refers to the last field id
	// so we rename it here for clarity
	// use default_spec_id for current partition spec id
	int32_t last_partition_field_id;
	bool has_last_partition_field_id = false;
	vector<SortOrder> sort_orders;
	bool has_sort_orders = false;
	int32_t default_sort_order_id;
	bool has_default_sort_order_id = false;
	vector<EncryptedKey> encryption_keys;
	bool has_encryption_keys = false;
	vector<Snapshot> snapshots;
	bool has_snapshots = false;
	SnapshotReferences refs;
	bool has_refs = false;
	int64_t current_snapshot_id;
	bool has_current_snapshot_id = false;
	int64_t last_sequence_number;
	bool has_last_sequence_number = false;
	SnapshotLog snapshot_log;
	bool has_snapshot_log = false;
	MetadataLog metadata_log;
	bool has_metadata_log = false;
	vector<StatisticsFile> statistics;
	bool has_statistics = false;
	vector<PartitionStatisticsFile> partition_statistics;
	bool has_partition_statistics = false;
};

} // namespace rest_api_objects
} // namespace duckdb
