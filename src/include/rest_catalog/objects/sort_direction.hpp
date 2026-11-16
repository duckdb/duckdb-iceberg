
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class SortDirection {
public:
	SortDirection();
	SortDirection(const SortDirection &) = delete;
	SortDirection &operator=(const SortDirection &) = delete;
	SortDirection(SortDirection &&) = default;
	SortDirection &operator=(SortDirection &&) = default;

public:
	// Deserialization
	static SortDirection FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SortDirection Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string value;
};

} // namespace rest_api_objects
} // namespace duckdb
