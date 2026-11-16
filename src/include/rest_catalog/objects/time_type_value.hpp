
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class TimeTypeValue {
public:
	TimeTypeValue();
	TimeTypeValue(const TimeTypeValue &) = delete;
	TimeTypeValue &operator=(const TimeTypeValue &) = delete;
	TimeTypeValue(TimeTypeValue &&) = default;
	TimeTypeValue &operator=(TimeTypeValue &&) = default;

public:
	// Deserialization
	static TimeTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	TimeTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
