
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/encrypted_key.hpp"
#include "rest_catalog/objects/metadata_log.hpp"
#include "rest_catalog/objects/partition_field.hpp"
#include "rest_catalog/objects/partition_spec.hpp"
#include "rest_catalog/objects/partition_statistics_file.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/snapshot.hpp"
#include "rest_catalog/objects/snapshot_log.hpp"
#include "rest_catalog/objects/snapshot_references.hpp"
#include "rest_catalog/objects/sort_order.hpp"
#include "rest_catalog/objects/statistics_file.hpp"

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
	// Deserialization
	static TableMetadata FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TableMetadata Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int32_t format_version;
	string table_uuid;
	optional<string> location;
	optional<int64_t> last_updated_ms;
	optional<int64_t> next_row_id;
	optional<case_insensitive_map_t<string>> properties;
	optional<vector<Schema>> schemas;
	optional<Schema> schema;
	optional<int32_t> current_schema_id;
	optional<int32_t> last_column_id;
	optional<vector<PartitionSpec>> partition_specs;
	optional<vector<PartitionField>> partition_spec;
	optional<int32_t> default_spec_id;
	optional<int32_t> last_partition_id;
	optional<vector<SortOrder>> sort_orders;
	optional<int32_t> default_sort_order_id;
	optional<vector<EncryptedKey>> encryption_keys;
	optional<vector<Snapshot>> snapshots;
	optional<SnapshotReferences> refs;
	optional<int64_t> current_snapshot_id;
	optional<int64_t> last_sequence_number;
	optional<SnapshotLog> snapshot_log;
	optional<MetadataLog> metadata_log;
	optional<vector<StatisticsFile>> statistics;
	optional<vector<PartitionStatisticsFile>> partition_statistics;
};

} // namespace rest_api_objects
} // namespace duckdb
