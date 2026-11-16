
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/transform.hpp"

namespace duckdb {
namespace rest_api_objects {

class PartitionField {
public:
	PartitionField();
	PartitionField(const PartitionField &) = delete;
	PartitionField &operator=(const PartitionField &) = delete;
	PartitionField(PartitionField &&) = default;
	PartitionField &operator=(PartitionField &&) = default;

public:
	// Deserialization
	static PartitionField FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	PartitionField Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int32_t source_id;
	Transform transform;
	string name;
	optional<int32_t> field_id;
};

} // namespace rest_api_objects
} // namespace duckdb
