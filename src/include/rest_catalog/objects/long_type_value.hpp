
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class LongTypeValue {
public:
	LongTypeValue();
	LongTypeValue(const LongTypeValue &) = delete;
	LongTypeValue &operator=(const LongTypeValue &) = delete;
	LongTypeValue(LongTypeValue &&) = default;
	LongTypeValue &operator=(LongTypeValue &&) = default;

public:
	// Deserialization
	static LongTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	LongTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int64_t value;
};

} // namespace rest_api_objects
} // namespace duckdb
