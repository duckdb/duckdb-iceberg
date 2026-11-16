
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

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
	// Deserialization
	static AssertLastAssignedPartitionId FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AssertLastAssignedPartitionId Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	int32_t last_assigned_partition_id;
};

} // namespace rest_api_objects
} // namespace duckdb
