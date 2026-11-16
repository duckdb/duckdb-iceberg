
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class RemoveSchemasUpdate {
public:
	RemoveSchemasUpdate();
	RemoveSchemasUpdate(const RemoveSchemasUpdate &) = delete;
	RemoveSchemasUpdate &operator=(const RemoveSchemasUpdate &) = delete;
	RemoveSchemasUpdate(RemoveSchemasUpdate &&) = default;
	RemoveSchemasUpdate &operator=(RemoveSchemasUpdate &&) = default;

public:
	// Deserialization
	static RemoveSchemasUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RemoveSchemasUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	vector<int32_t> schema_ids;
};

} // namespace rest_api_objects
} // namespace duckdb
