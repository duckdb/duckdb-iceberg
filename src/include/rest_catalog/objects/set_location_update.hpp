
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class SetLocationUpdate {
public:
	SetLocationUpdate();
	SetLocationUpdate(const SetLocationUpdate &) = delete;
	SetLocationUpdate &operator=(const SetLocationUpdate &) = delete;
	SetLocationUpdate(SetLocationUpdate &&) = default;
	SetLocationUpdate &operator=(SetLocationUpdate &&) = default;

public:
	// Deserialization
	static SetLocationUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SetLocationUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	string location;
};

} // namespace rest_api_objects
} // namespace duckdb
