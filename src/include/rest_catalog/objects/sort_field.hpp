
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/null_order.hpp"
#include "rest_catalog/objects/sort_direction.hpp"
#include "rest_catalog/objects/transform.hpp"

namespace duckdb {
namespace rest_api_objects {

class SortField {
public:
	SortField();
	SortField(const SortField &) = delete;
	SortField &operator=(const SortField &) = delete;
	SortField(SortField &&) = default;
	SortField &operator=(SortField &&) = default;

public:
	// Deserialization
	static SortField FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SortField Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int32_t source_id;
	Transform transform;
	SortDirection direction;
	NullOrder null_order;
};

} // namespace rest_api_objects
} // namespace duckdb
