
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class AssertCurrentSchemaId {
public:
	AssertCurrentSchemaId();
	AssertCurrentSchemaId(const AssertCurrentSchemaId &) = delete;
	AssertCurrentSchemaId &operator=(const AssertCurrentSchemaId &) = delete;
	AssertCurrentSchemaId(AssertCurrentSchemaId &&) = default;
	AssertCurrentSchemaId &operator=(AssertCurrentSchemaId &&) = default;

public:
	// Deserialization
	static AssertCurrentSchemaId FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AssertCurrentSchemaId Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	int32_t current_schema_id;
};

} // namespace rest_api_objects
} // namespace duckdb
