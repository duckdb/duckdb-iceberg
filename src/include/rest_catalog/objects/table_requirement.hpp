
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/assert_create.hpp"
#include "rest_catalog/objects/assert_current_schema_id.hpp"
#include "rest_catalog/objects/assert_default_sort_order_id.hpp"
#include "rest_catalog/objects/assert_default_spec_id.hpp"
#include "rest_catalog/objects/assert_last_assigned_field_id.hpp"
#include "rest_catalog/objects/assert_last_assigned_partition_id.hpp"
#include "rest_catalog/objects/assert_ref_snapshot_id.hpp"
#include "rest_catalog/objects/assert_table_uuid.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TableRequirement {
public:
	TableRequirement();
	TableRequirement(const TableRequirement &) = delete;
	TableRequirement &operator=(const TableRequirement &) = delete;
	TableRequirement(TableRequirement &&) = default;
	TableRequirement &operator=(TableRequirement &&) = default;

public:
	static TableRequirement FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	AssertCreate assert_create;
	bool has_assert_create = false;
	AssertTableUUID assert_table_uuid;
	bool has_assert_table_uuid = false;
	AssertRefSnapshotId assert_ref_snapshot_id;
	bool has_assert_ref_snapshot_id = false;
	AssertLastAssignedFieldId assert_last_assigned_field_id;
	bool has_assert_last_assigned_field_id = false;
	AssertCurrentSchemaId assert_current_schema_id;
	bool has_assert_current_schema_id = false;
	AssertLastAssignedPartitionId assert_last_assigned_partition_id;
	bool has_assert_last_assigned_partition_id = false;
	AssertDefaultSpecId assert_default_spec_id;
	bool has_assert_default_spec_id = false;
	AssertDefaultSortOrderId assert_default_sort_order_id;
	bool has_assert_default_sort_order_id = false;
};

} // namespace rest_api_objects
} // namespace duckdb
