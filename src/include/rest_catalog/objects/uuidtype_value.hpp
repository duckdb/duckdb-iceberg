
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class UUIDTypeValue {
public:
	UUIDTypeValue();
	UUIDTypeValue(const UUIDTypeValue &) = delete;
	UUIDTypeValue &operator=(const UUIDTypeValue &) = delete;
	UUIDTypeValue(UUIDTypeValue &&) = default;
	UUIDTypeValue &operator=(UUIDTypeValue &&) = default;

public:
	// Deserialization
	static UUIDTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	UUIDTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
