
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/counter_result.hpp"
#include "rest_catalog/objects/timer_result.hpp"

namespace duckdb {
namespace rest_api_objects {

class MetricResult {
public:
	MetricResult();
	MetricResult(const MetricResult &) = delete;
	MetricResult &operator=(const MetricResult &) = delete;
	MetricResult(MetricResult &&) = default;
	MetricResult &operator=(MetricResult &&) = default;

public:
	// Deserialization
	static MetricResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	MetricResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<CounterResult> counter_result;
	optional<TimerResult> timer_result;
};

} // namespace rest_api_objects
} // namespace duckdb
