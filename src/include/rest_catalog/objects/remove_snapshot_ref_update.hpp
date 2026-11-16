
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class RemoveSnapshotRefUpdate {
public:
	RemoveSnapshotRefUpdate();
	RemoveSnapshotRefUpdate(const RemoveSnapshotRefUpdate &) = delete;
	RemoveSnapshotRefUpdate &operator=(const RemoveSnapshotRefUpdate &) = delete;
	RemoveSnapshotRefUpdate(RemoveSnapshotRefUpdate &&) = default;
	RemoveSnapshotRefUpdate &operator=(RemoveSnapshotRefUpdate &&) = default;

public:
	// Deserialization
	static RemoveSnapshotRefUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RemoveSnapshotRefUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	string ref_name;
};

} // namespace rest_api_objects
} // namespace duckdb
