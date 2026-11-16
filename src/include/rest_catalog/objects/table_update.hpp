
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/add_encryption_key_update.hpp"
#include "rest_catalog/objects/add_partition_spec_update.hpp"
#include "rest_catalog/objects/add_schema_update.hpp"
#include "rest_catalog/objects/add_snapshot_update.hpp"
#include "rest_catalog/objects/add_sort_order_update.hpp"
#include "rest_catalog/objects/assign_uuidupdate.hpp"
#include "rest_catalog/objects/remove_encryption_key_update.hpp"
#include "rest_catalog/objects/remove_partition_specs_update.hpp"
#include "rest_catalog/objects/remove_partition_statistics_update.hpp"
#include "rest_catalog/objects/remove_properties_update.hpp"
#include "rest_catalog/objects/remove_schemas_update.hpp"
#include "rest_catalog/objects/remove_snapshot_ref_update.hpp"
#include "rest_catalog/objects/remove_snapshots_update.hpp"
#include "rest_catalog/objects/remove_statistics_update.hpp"
#include "rest_catalog/objects/set_current_schema_update.hpp"
#include "rest_catalog/objects/set_default_sort_order_update.hpp"
#include "rest_catalog/objects/set_default_spec_update.hpp"
#include "rest_catalog/objects/set_location_update.hpp"
#include "rest_catalog/objects/set_partition_statistics_update.hpp"
#include "rest_catalog/objects/set_properties_update.hpp"
#include "rest_catalog/objects/set_snapshot_ref_update.hpp"
#include "rest_catalog/objects/set_statistics_update.hpp"
#include "rest_catalog/objects/upgrade_format_version_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class TableUpdate {
public:
	TableUpdate();
	TableUpdate(const TableUpdate &) = delete;
	TableUpdate &operator=(const TableUpdate &) = delete;
	TableUpdate(TableUpdate &&) = default;
	TableUpdate &operator=(TableUpdate &&) = default;

public:
	// Deserialization
	static TableUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TableUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<AssignUUIDUpdate> assign_uuidupdate;
	optional<UpgradeFormatVersionUpdate> upgrade_format_version_update;
	optional<AddSchemaUpdate> add_schema_update;
	optional<SetCurrentSchemaUpdate> set_current_schema_update;
	optional<AddPartitionSpecUpdate> add_partition_spec_update;
	optional<SetDefaultSpecUpdate> set_default_spec_update;
	optional<AddSortOrderUpdate> add_sort_order_update;
	optional<SetDefaultSortOrderUpdate> set_default_sort_order_update;
	optional<AddSnapshotUpdate> add_snapshot_update;
	optional<SetSnapshotRefUpdate> set_snapshot_ref_update;
	optional<RemoveSnapshotsUpdate> remove_snapshots_update;
	optional<RemoveSnapshotRefUpdate> remove_snapshot_ref_update;
	optional<SetLocationUpdate> set_location_update;
	optional<SetPropertiesUpdate> set_properties_update;
	optional<RemovePropertiesUpdate> remove_properties_update;
	optional<SetStatisticsUpdate> set_statistics_update;
	optional<RemoveStatisticsUpdate> remove_statistics_update;
	optional<SetPartitionStatisticsUpdate> set_partition_statistics_update;
	optional<RemovePartitionStatisticsUpdate> remove_partition_statistics_update;
	optional<RemovePartitionSpecsUpdate> remove_partition_specs_update;
	optional<RemoveSchemasUpdate> remove_schemas_update;
	optional<AddEncryptionKeyUpdate> add_encryption_key_update;
	optional<RemoveEncryptionKeyUpdate> remove_encryption_key_update;
};

} // namespace rest_api_objects
} // namespace duckdb
