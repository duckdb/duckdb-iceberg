
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/table_metadata.hpp"

namespace duckdb {
namespace rest_api_objects {

class UnregisterTableResult {
public:
	UnregisterTableResult();
	UnregisterTableResult(const UnregisterTableResult &) = delete;
	UnregisterTableResult &operator=(const UnregisterTableResult &) = delete;
	UnregisterTableResult(UnregisterTableResult &&) = default;
	UnregisterTableResult &operator=(UnregisterTableResult &&) = default;

public:
	// Deserialization
	static UnregisterTableResult FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	UnregisterTableResult Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string metadata_location;
	TableMetadata metadata;
};

} // namespace rest_api_objects
} // namespace duckdb
