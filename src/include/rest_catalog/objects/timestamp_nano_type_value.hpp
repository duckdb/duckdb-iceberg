
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class TimestampNanoTypeValue {
public:
	TimestampNanoTypeValue();
	TimestampNanoTypeValue(const TimestampNanoTypeValue &) = delete;
	TimestampNanoTypeValue &operator=(const TimestampNanoTypeValue &) = delete;
	TimestampNanoTypeValue(TimestampNanoTypeValue &&) = default;
	TimestampNanoTypeValue &operator=(TimestampNanoTypeValue &&) = default;

public:
	// Deserialization
	static TimestampNanoTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TimestampNanoTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
