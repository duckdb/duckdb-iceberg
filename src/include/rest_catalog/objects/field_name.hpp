
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class FieldName {
public:
	FieldName();
	FieldName(const FieldName &) = delete;
	FieldName &operator=(const FieldName &) = delete;
	FieldName(FieldName &&) = default;
	FieldName &operator=(FieldName &&) = default;

public:
	// Deserialization
	static FieldName FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	FieldName Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
