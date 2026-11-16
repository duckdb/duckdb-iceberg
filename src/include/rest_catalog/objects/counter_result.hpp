
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class CounterResult {
public:
	CounterResult();
	CounterResult(const CounterResult &) = delete;
	CounterResult &operator=(const CounterResult &) = delete;
	CounterResult(CounterResult &&) = default;
	CounterResult &operator=(CounterResult &&) = default;

public:
	// Deserialization
	static CounterResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	CounterResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string unit;
	int64_t value;
};

} // namespace rest_api_objects
} // namespace duckdb
