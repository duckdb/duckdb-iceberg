
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class TimerResult {
public:
	TimerResult();
	TimerResult(const TimerResult &) = delete;
	TimerResult &operator=(const TimerResult &) = delete;
	TimerResult(TimerResult &&) = default;
	TimerResult &operator=(TimerResult &&) = default;

public:
	// Deserialization
	static TimerResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TimerResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string time_unit;
	int64_t count;
	int64_t total_duration;
};

} // namespace rest_api_objects
} // namespace duckdb
