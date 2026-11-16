
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/partition_field.hpp"

namespace duckdb {
namespace rest_api_objects {

class PartitionSpec {
public:
	PartitionSpec();
	PartitionSpec(const PartitionSpec &) = delete;
	PartitionSpec &operator=(const PartitionSpec &) = delete;
	PartitionSpec(PartitionSpec &&) = default;
	PartitionSpec &operator=(PartitionSpec &&) = default;

public:
	// Deserialization
	static PartitionSpec FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	PartitionSpec Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	vector<PartitionField> fields;
	optional<int32_t> spec_id;
};

} // namespace rest_api_objects
} // namespace duckdb
