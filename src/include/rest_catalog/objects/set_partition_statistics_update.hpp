
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/partition_statistics_file.hpp"

namespace duckdb {
namespace rest_api_objects {

class SetPartitionStatisticsUpdate {
public:
	SetPartitionStatisticsUpdate();
	SetPartitionStatisticsUpdate(const SetPartitionStatisticsUpdate &) = delete;
	SetPartitionStatisticsUpdate &operator=(const SetPartitionStatisticsUpdate &) = delete;
	SetPartitionStatisticsUpdate(SetPartitionStatisticsUpdate &&) = default;
	SetPartitionStatisticsUpdate &operator=(SetPartitionStatisticsUpdate &&) = default;

public:
	// Deserialization
	static SetPartitionStatisticsUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SetPartitionStatisticsUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	PartitionStatisticsFile partition_statistics;
};

} // namespace rest_api_objects
} // namespace duckdb
