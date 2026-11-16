
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class DecimalTypeValue {
public:
	DecimalTypeValue();
	DecimalTypeValue(const DecimalTypeValue &) = delete;
	DecimalTypeValue &operator=(const DecimalTypeValue &) = delete;
	DecimalTypeValue(DecimalTypeValue &&) = default;
	DecimalTypeValue &operator=(DecimalTypeValue &&) = default;

public:
	// Deserialization
	static DecimalTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	DecimalTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
