
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class AssertDefaultSortOrderId {
public:
	AssertDefaultSortOrderId();
	AssertDefaultSortOrderId(const AssertDefaultSortOrderId &) = delete;
	AssertDefaultSortOrderId &operator=(const AssertDefaultSortOrderId &) = delete;
	AssertDefaultSortOrderId(AssertDefaultSortOrderId &&) = default;
	AssertDefaultSortOrderId &operator=(AssertDefaultSortOrderId &&) = default;

public:
	// Deserialization
	static AssertDefaultSortOrderId FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AssertDefaultSortOrderId Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	int32_t default_sort_order_id;
};

} // namespace rest_api_objects
} // namespace duckdb
