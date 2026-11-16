
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/assert_create.hpp"
#include "rest_catalog/objects/assert_current_schema_id.hpp"
#include "rest_catalog/objects/assert_default_sort_order_id.hpp"
#include "rest_catalog/objects/assert_default_spec_id.hpp"
#include "rest_catalog/objects/assert_last_assigned_field_id.hpp"
#include "rest_catalog/objects/assert_last_assigned_partition_id.hpp"
#include "rest_catalog/objects/assert_ref_snapshot_id.hpp"
#include "rest_catalog/objects/assert_table_uuid.hpp"

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
	// Deserialization
	static TableRequirement FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TableRequirement Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<AssertCreate> assert_create;
	optional<AssertTableUUID> assert_table_uuid;
	optional<AssertRefSnapshotId> assert_ref_snapshot_id;
	optional<AssertLastAssignedFieldId> assert_last_assigned_field_id;
	optional<AssertCurrentSchemaId> assert_current_schema_id;
	optional<AssertLastAssignedPartitionId> assert_last_assigned_partition_id;
	optional<AssertDefaultSpecId> assert_default_spec_id;
	optional<AssertDefaultSortOrderId> assert_default_sort_order_id;
};

} // namespace rest_api_objects
} // namespace duckdb
