
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class TimestampTypeValue {
public:
	TimestampTypeValue();
	TimestampTypeValue(const TimestampTypeValue &) = delete;
	TimestampTypeValue &operator=(const TimestampTypeValue &) = delete;
	TimestampTypeValue(TimestampTypeValue &&) = default;
	TimestampTypeValue &operator=(TimestampTypeValue &&) = default;

public:
	// Deserialization
	static TimestampTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TimestampTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
