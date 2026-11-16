
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class FixedTypeValue {
public:
	FixedTypeValue();
	FixedTypeValue(const FixedTypeValue &) = delete;
	FixedTypeValue &operator=(const FixedTypeValue &) = delete;
	FixedTypeValue(FixedTypeValue &&) = default;
	FixedTypeValue &operator=(FixedTypeValue &&) = default;

public:
	// Deserialization
	static FixedTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FixedTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
