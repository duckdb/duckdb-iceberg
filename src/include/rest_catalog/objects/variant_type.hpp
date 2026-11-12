
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class VariantType {
public:
	VariantType();
	VariantType(const VariantType &) = delete;
	VariantType &operator=(const VariantType &) = delete;
	VariantType(VariantType &&) = default;
	VariantType &operator=(VariantType &&) = default;

public:
	// Deserialization
	static VariantType FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	VariantType Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
