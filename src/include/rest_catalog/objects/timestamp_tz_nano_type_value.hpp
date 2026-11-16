
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class TimestampTzNanoTypeValue {
public:
	TimestampTzNanoTypeValue();
	TimestampTzNanoTypeValue(const TimestampTzNanoTypeValue &) = delete;
	TimestampTzNanoTypeValue &operator=(const TimestampTzNanoTypeValue &) = delete;
	TimestampTzNanoTypeValue(TimestampTzNanoTypeValue &&) = default;
	TimestampTzNanoTypeValue &operator=(TimestampTzNanoTypeValue &&) = default;

public:
	// Deserialization
	static TimestampTzNanoTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TimestampTzNanoTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
