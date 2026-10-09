
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/metrics.hpp"

namespace duckdb {
namespace rest_api_objects {

class Predicate;

class ScanReport {
public:
	ScanReport();
	ScanReport(const ScanReport &) = delete;
	ScanReport &operator=(const ScanReport &) = delete;
	ScanReport(ScanReport &&) = default;
	ScanReport &operator=(ScanReport &&) = default;

public:
	// Deserialization
	static ScanReport FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ScanReport Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string table_name;
	int64_t snapshot_id;
	unique_ptr<Predicate> filter;
	int32_t schema_id;
	vector<int32_t> projected_field_ids;
	vector<string> projected_field_names;
	Metrics metrics;
	optional<case_insensitive_map_t<string>> metadata;
};

} // namespace rest_api_objects
} // namespace duckdb
