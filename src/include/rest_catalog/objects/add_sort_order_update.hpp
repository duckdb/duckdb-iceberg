
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/sort_order.hpp"

namespace duckdb {
namespace rest_api_objects {

class AddSortOrderUpdate {
public:
	AddSortOrderUpdate();
	AddSortOrderUpdate(const AddSortOrderUpdate &) = delete;
	AddSortOrderUpdate &operator=(const AddSortOrderUpdate &) = delete;
	AddSortOrderUpdate(AddSortOrderUpdate &&) = default;
	AddSortOrderUpdate &operator=(AddSortOrderUpdate &&) = default;

public:
	// Deserialization
	static AddSortOrderUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AddSortOrderUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	SortOrder sort_order;
};

} // namespace rest_api_objects
} // namespace duckdb
