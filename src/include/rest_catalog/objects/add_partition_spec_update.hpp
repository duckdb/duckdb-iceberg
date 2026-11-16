
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/partition_spec.hpp"

namespace duckdb {
namespace rest_api_objects {

class AddPartitionSpecUpdate {
public:
	AddPartitionSpecUpdate();
	AddPartitionSpecUpdate(const AddPartitionSpecUpdate &) = delete;
	AddPartitionSpecUpdate &operator=(const AddPartitionSpecUpdate &) = delete;
	AddPartitionSpecUpdate(AddPartitionSpecUpdate &&) = default;
	AddPartitionSpecUpdate &operator=(AddPartitionSpecUpdate &&) = default;

public:
	// Deserialization
	static AddPartitionSpecUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AddPartitionSpecUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	PartitionSpec spec;
};

} // namespace rest_api_objects
} // namespace duckdb
