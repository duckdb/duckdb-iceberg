
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class BooleanTypeValue {
public:
	BooleanTypeValue();
	BooleanTypeValue(const BooleanTypeValue &) = delete;
	BooleanTypeValue &operator=(const BooleanTypeValue &) = delete;
	BooleanTypeValue(BooleanTypeValue &&) = default;
	BooleanTypeValue &operator=(BooleanTypeValue &&) = default;

public:
	// Deserialization
	static BooleanTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	BooleanTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	bool value;
};

} // namespace rest_api_objects
} // namespace duckdb
