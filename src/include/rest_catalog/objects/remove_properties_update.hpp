
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class RemovePropertiesUpdate {
public:
	RemovePropertiesUpdate();
	RemovePropertiesUpdate(const RemovePropertiesUpdate &) = delete;
	RemovePropertiesUpdate &operator=(const RemovePropertiesUpdate &) = delete;
	RemovePropertiesUpdate(RemovePropertiesUpdate &&) = default;
	RemovePropertiesUpdate &operator=(RemovePropertiesUpdate &&) = default;

public:
	// Deserialization
	static RemovePropertiesUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RemovePropertiesUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	vector<string> removals;
};

} // namespace rest_api_objects
} // namespace duckdb
