
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/metrics.hpp"

namespace duckdb {
namespace rest_api_objects {

class CommitReport {
public:
	CommitReport();
	CommitReport(const CommitReport &) = delete;
	CommitReport &operator=(const CommitReport &) = delete;
	CommitReport(CommitReport &&) = default;
	CommitReport &operator=(CommitReport &&) = default;

public:
	// Deserialization
	static CommitReport FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CommitReport Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string table_name;
	int64_t snapshot_id;
	int64_t sequence_number;
	string operation;
	Metrics metrics;
	optional<unordered_map<string, string>> metadata;
};

} // namespace rest_api_objects
} // namespace duckdb
