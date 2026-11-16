
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class AssignUUIDUpdate {
public:
	AssignUUIDUpdate();
	AssignUUIDUpdate(const AssignUUIDUpdate &) = delete;
	AssignUUIDUpdate &operator=(const AssignUUIDUpdate &) = delete;
	AssignUUIDUpdate(AssignUUIDUpdate &&) = default;
	AssignUUIDUpdate &operator=(AssignUUIDUpdate &&) = default;

public:
	// Deserialization
	static AssignUUIDUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AssignUUIDUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	string uuid;
};

} // namespace rest_api_objects
} // namespace duckdb
