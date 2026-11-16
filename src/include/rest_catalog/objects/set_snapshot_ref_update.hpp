
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/snapshot_reference.hpp"

namespace duckdb {
namespace rest_api_objects {

class SetSnapshotRefUpdate {
public:
	SetSnapshotRefUpdate();
	SetSnapshotRefUpdate(const SetSnapshotRefUpdate &) = delete;
	SetSnapshotRefUpdate &operator=(const SetSnapshotRefUpdate &) = delete;
	SetSnapshotRefUpdate(SetSnapshotRefUpdate &&) = default;
	SetSnapshotRefUpdate &operator=(SetSnapshotRefUpdate &&) = default;

public:
	// Deserialization
	static SetSnapshotRefUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SetSnapshotRefUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	SnapshotReference snapshot_reference;
	string ref_name;
};

} // namespace rest_api_objects
} // namespace duckdb
