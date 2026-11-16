
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class SetPropertiesUpdate {
public:
	SetPropertiesUpdate();
	SetPropertiesUpdate(const SetPropertiesUpdate &) = delete;
	SetPropertiesUpdate &operator=(const SetPropertiesUpdate &) = delete;
	SetPropertiesUpdate(SetPropertiesUpdate &&) = default;
	SetPropertiesUpdate &operator=(SetPropertiesUpdate &&) = default;

public:
	// Deserialization
	static SetPropertiesUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SetPropertiesUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	unordered_map<string, string> updates;
};

} // namespace rest_api_objects
} // namespace duckdb
