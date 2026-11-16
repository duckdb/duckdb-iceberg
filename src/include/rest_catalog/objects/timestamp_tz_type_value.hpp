
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class TimestampTzTypeValue {
public:
	TimestampTzTypeValue();
	TimestampTzTypeValue(const TimestampTzTypeValue &) = delete;
	TimestampTzTypeValue &operator=(const TimestampTzTypeValue &) = delete;
	TimestampTzTypeValue(TimestampTzTypeValue &&) = default;
	TimestampTzTypeValue &operator=(TimestampTzTypeValue &&) = default;

public:
	// Deserialization
	static TimestampTzTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TimestampTzTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
