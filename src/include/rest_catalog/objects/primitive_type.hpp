
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class PrimitiveType {
public:
	PrimitiveType();
	PrimitiveType(const PrimitiveType &) = delete;
	PrimitiveType &operator=(const PrimitiveType &) = delete;
	PrimitiveType(PrimitiveType &&) = default;
	PrimitiveType &operator=(PrimitiveType &&) = default;

public:
	// Deserialization
	static PrimitiveType FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	PrimitiveType Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
