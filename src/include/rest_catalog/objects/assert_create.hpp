
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class AssertCreate {
public:
	AssertCreate();
	AssertCreate(const AssertCreate &) = delete;
	AssertCreate &operator=(const AssertCreate &) = delete;
	AssertCreate(AssertCreate &&) = default;
	AssertCreate &operator=(AssertCreate &&) = default;

public:
	// Deserialization
	static AssertCreate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AssertCreate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
