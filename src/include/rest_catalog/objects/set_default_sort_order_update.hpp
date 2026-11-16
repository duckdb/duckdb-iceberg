
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class SetDefaultSortOrderUpdate {
public:
	SetDefaultSortOrderUpdate();
	SetDefaultSortOrderUpdate(const SetDefaultSortOrderUpdate &) = delete;
	SetDefaultSortOrderUpdate &operator=(const SetDefaultSortOrderUpdate &) = delete;
	SetDefaultSortOrderUpdate(SetDefaultSortOrderUpdate &&) = default;
	SetDefaultSortOrderUpdate &operator=(SetDefaultSortOrderUpdate &&) = default;

public:
	// Deserialization
	static SetDefaultSortOrderUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SetDefaultSortOrderUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	int32_t sort_order_id;
};

} // namespace rest_api_objects
} // namespace duckdb
