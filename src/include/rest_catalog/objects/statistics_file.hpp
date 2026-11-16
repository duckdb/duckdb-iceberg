
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/blob_metadata.hpp"

namespace duckdb {
namespace rest_api_objects {

class StatisticsFile {
public:
	StatisticsFile();
	StatisticsFile(const StatisticsFile &) = delete;
	StatisticsFile &operator=(const StatisticsFile &) = delete;
	StatisticsFile(StatisticsFile &&) = default;
	StatisticsFile &operator=(StatisticsFile &&) = default;

public:
	// Deserialization
	static StatisticsFile FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	StatisticsFile Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int64_t snapshot_id;
	string statistics_path;
	int64_t file_size_in_bytes;
	int64_t file_footer_size_in_bytes;
	vector<BlobMetadata> blob_metadata;
};

} // namespace rest_api_objects
} // namespace duckdb
