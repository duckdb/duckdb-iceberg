
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class PartitionStatisticsFile {
public:
	PartitionStatisticsFile();
	PartitionStatisticsFile(const PartitionStatisticsFile &) = delete;
	PartitionStatisticsFile &operator=(const PartitionStatisticsFile &) = delete;
	PartitionStatisticsFile(PartitionStatisticsFile &&) = default;
	PartitionStatisticsFile &operator=(PartitionStatisticsFile &&) = default;

public:
	// Deserialization
	static PartitionStatisticsFile FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	PartitionStatisticsFile Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int64_t snapshot_id;
	string statistics_path;
	int64_t file_size_in_bytes;
};

} // namespace rest_api_objects
} // namespace duckdb
