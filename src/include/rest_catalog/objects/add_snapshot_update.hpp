
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"
#include "rest_catalog/objects/snapshot.hpp"

namespace duckdb {
namespace rest_api_objects {

class AddSnapshotUpdate {
public:
	AddSnapshotUpdate();
	AddSnapshotUpdate(const AddSnapshotUpdate &) = delete;
	AddSnapshotUpdate &operator=(const AddSnapshotUpdate &) = delete;
	AddSnapshotUpdate(AddSnapshotUpdate &&) = default;
	AddSnapshotUpdate &operator=(AddSnapshotUpdate &&) = default;

public:
	// Deserialization
	static AddSnapshotUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AddSnapshotUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	Snapshot snapshot;
};

} // namespace rest_api_objects
} // namespace duckdb
