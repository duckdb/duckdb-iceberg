
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class RemovePartitionStatisticsUpdate {
public:
	RemovePartitionStatisticsUpdate();
	RemovePartitionStatisticsUpdate(const RemovePartitionStatisticsUpdate &) = delete;
	RemovePartitionStatisticsUpdate &operator=(const RemovePartitionStatisticsUpdate &) = delete;
	RemovePartitionStatisticsUpdate(RemovePartitionStatisticsUpdate &&) = default;
	RemovePartitionStatisticsUpdate &operator=(RemovePartitionStatisticsUpdate &&) = default;

public:
	// Deserialization
	static RemovePartitionStatisticsUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RemovePartitionStatisticsUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	int64_t snapshot_id;
};

} // namespace rest_api_objects
} // namespace duckdb
