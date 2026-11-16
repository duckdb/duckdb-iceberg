
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class FloatTypeValue {
public:
	FloatTypeValue();
	FloatTypeValue(const FloatTypeValue &) = delete;
	FloatTypeValue &operator=(const FloatTypeValue &) = delete;
	FloatTypeValue(FloatTypeValue &&) = default;
	FloatTypeValue &operator=(FloatTypeValue &&) = default;

public:
	// Deserialization
	static FloatTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FloatTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	double value;
};

} // namespace rest_api_objects
} // namespace duckdb
