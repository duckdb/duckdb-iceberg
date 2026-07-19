
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

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
	// Deserialization
	static AssertLastAssignedPartitionId FromJSON(yyjson_val *obj);
	string TryFromJSON(yyjson_val *obj);

	// Copy
	AssertLastAssignedPartitionId Copy() const;

	// Serialization
	void PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const;
	yyjson_mut_val *ToJSON(yyjson_mut_doc *doc) const;

public:
	string type;
	int32_t last_assigned_partition_id;
};

} // namespace rest_api_objects
} // namespace duckdb
