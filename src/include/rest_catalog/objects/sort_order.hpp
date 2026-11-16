
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/sort_field.hpp"

namespace duckdb {
namespace rest_api_objects {

class SortOrder {
public:
	SortOrder();
	SortOrder(const SortOrder &) = delete;
	SortOrder &operator=(const SortOrder &) = delete;
	SortOrder(SortOrder &&) = default;
	SortOrder &operator=(SortOrder &&) = default;

public:
	// Deserialization
	static SortOrder FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SortOrder Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int32_t order_id;
	vector<SortField> fields;
};

} // namespace rest_api_objects
} // namespace duckdb
