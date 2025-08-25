
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/table_requirement_type.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class AssertLastAssignedPartitionId {
public:
	AssertLastAssignedPartitionId();
	AssertLastAssignedPartitionId(const AssertLastAssignedPartitionId &) = delete;
	AssertLastAssignedPartitionId &operator=(const AssertLastAssignedPartitionId &) = delete;
	AssertLastAssignedPartitionId(AssertLastAssignedPartitionId &&) = default;
	AssertLastAssignedPartitionId &operator=(AssertLastAssignedPartitionId &&) = default;

public:
	static AssertLastAssignedPartitionId FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	TableRequirementType type;
	int32_t last_assigned_partition_id;
};

} // namespace rest_api_objects
} // namespace duckdb
