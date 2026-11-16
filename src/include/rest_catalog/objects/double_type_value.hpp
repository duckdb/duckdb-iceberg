
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class DoubleTypeValue {
public:
	DoubleTypeValue();
	DoubleTypeValue(const DoubleTypeValue &) = delete;
	DoubleTypeValue &operator=(const DoubleTypeValue &) = delete;
	DoubleTypeValue(DoubleTypeValue &&) = default;
	DoubleTypeValue &operator=(DoubleTypeValue &&) = default;

public:
	// Deserialization
	static DoubleTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	DoubleTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	double value;
};

} // namespace rest_api_objects
} // namespace duckdb
