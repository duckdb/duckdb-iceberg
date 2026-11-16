
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class StringTypeValue {
public:
	StringTypeValue();
	StringTypeValue(const StringTypeValue &) = delete;
	StringTypeValue &operator=(const StringTypeValue &) = delete;
	StringTypeValue(StringTypeValue &&) = default;
	StringTypeValue &operator=(StringTypeValue &&) = default;

public:
	// Deserialization
	static StringTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	StringTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
