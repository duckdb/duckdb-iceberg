
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class BinaryTypeValue {
public:
	BinaryTypeValue();
	BinaryTypeValue(const BinaryTypeValue &) = delete;
	BinaryTypeValue &operator=(const BinaryTypeValue &) = delete;
	BinaryTypeValue(BinaryTypeValue &&) = default;
	BinaryTypeValue &operator=(BinaryTypeValue &&) = default;

public:
	// Deserialization
	static BinaryTypeValue FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	BinaryTypeValue Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
