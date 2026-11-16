
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class RemovePartitionSpecsUpdate {
public:
	RemovePartitionSpecsUpdate();
	RemovePartitionSpecsUpdate(const RemovePartitionSpecsUpdate &) = delete;
	RemovePartitionSpecsUpdate &operator=(const RemovePartitionSpecsUpdate &) = delete;
	RemovePartitionSpecsUpdate(RemovePartitionSpecsUpdate &&) = default;
	RemovePartitionSpecsUpdate &operator=(RemovePartitionSpecsUpdate &&) = default;

public:
	// Deserialization
	static RemovePartitionSpecsUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RemovePartitionSpecsUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	vector<int32_t> spec_ids;
};

} // namespace rest_api_objects
} // namespace duckdb
