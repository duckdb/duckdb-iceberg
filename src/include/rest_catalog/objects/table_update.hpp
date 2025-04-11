
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/add_partition_spec_update.hpp"
#include "rest_catalog/objects/add_schema_update.hpp"
#include "rest_catalog/objects/add_snapshot_update.hpp"
#include "rest_catalog/objects/add_sort_order_update.hpp"
#include "rest_catalog/objects/assign_uuidupdate.hpp"
#include "rest_catalog/objects/enable_row_lineage_update.hpp"
#include "rest_catalog/objects/remove_partition_specs_update.hpp"
#include "rest_catalog/objects/remove_properties_update.hpp"
#include "rest_catalog/objects/remove_schemas_update.hpp"
#include "rest_catalog/objects/remove_snapshot_ref_update.hpp"
#include "rest_catalog/objects/remove_snapshots_update.hpp"
#include "rest_catalog/objects/remove_statistics_update.hpp"
#include "rest_catalog/objects/set_current_schema_update.hpp"
#include "rest_catalog/objects/set_default_sort_order_update.hpp"
#include "rest_catalog/objects/set_default_spec_update.hpp"
#include "rest_catalog/objects/set_location_update.hpp"
#include "rest_catalog/objects/set_properties_update.hpp"
#include "rest_catalog/objects/set_snapshot_ref_update.hpp"
#include "rest_catalog/objects/set_statistics_update.hpp"
#include "rest_catalog/objects/upgrade_format_version_update.hpp"

using namespace duckdb_yyjson;

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
	static TableUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	AssignUUIDUpdate assign_uuidupdate;
	bool has_assign_uuidupdate = false;
	UpgradeFormatVersionUpdate upgrade_format_version_update;
	bool has_upgrade_format_version_update = false;
	AddSchemaUpdate add_schema_update;
	bool has_add_schema_update = false;
	SetCurrentSchemaUpdate set_current_schema_update;
	bool has_set_current_schema_update = false;
	AddPartitionSpecUpdate add_partition_spec_update;
	bool has_add_partition_spec_update = false;
	SetDefaultSpecUpdate set_default_spec_update;
	bool has_set_default_spec_update = false;
	AddSortOrderUpdate add_sort_order_update;
	bool has_add_sort_order_update = false;
	SetDefaultSortOrderUpdate set_default_sort_order_update;
	bool has_set_default_sort_order_update = false;
	AddSnapshotUpdate add_snapshot_update;
	bool has_add_snapshot_update = false;
	SetSnapshotRefUpdate set_snapshot_ref_update;
	bool has_set_snapshot_ref_update = false;
	RemoveSnapshotsUpdate remove_snapshots_update;
	bool has_remove_snapshots_update = false;
	RemoveSnapshotRefUpdate remove_snapshot_ref_update;
	bool has_remove_snapshot_ref_update = false;
	SetLocationUpdate set_location_update;
	bool has_set_location_update = false;
	SetPropertiesUpdate set_properties_update;
	bool has_set_properties_update = false;
	RemovePropertiesUpdate remove_properties_update;
	bool has_remove_properties_update = false;
	SetStatisticsUpdate set_statistics_update;
	bool has_set_statistics_update = false;
	RemoveStatisticsUpdate remove_statistics_update;
	bool has_remove_statistics_update = false;
	RemovePartitionSpecsUpdate remove_partition_specs_update;
	bool has_remove_partition_specs_update = false;
	RemoveSchemasUpdate remove_schemas_update;
	bool has_remove_schemas_update = false;
	EnableRowLineageUpdate enable_row_lineage_update;
	bool has_enable_row_lineage_update = false;
};

} // namespace rest_api_objects
} // namespace duckdb
