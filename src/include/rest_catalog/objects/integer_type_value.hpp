
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class IntegerTypeValue {
public:
	IntegerTypeValue();
	IntegerTypeValue(const IntegerTypeValue &) = delete;
	IntegerTypeValue &operator=(const IntegerTypeValue &) = delete;
	IntegerTypeValue(IntegerTypeValue &&) = default;
	IntegerTypeValue &operator=(IntegerTypeValue &&) = default;

public:
	// Deserialization
	static IntegerTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	IntegerTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int32_t value;
};

} // namespace rest_api_objects
} // namespace duckdb
