
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/base_update.hpp"

namespace duckdb {
namespace rest_api_objects {

class RemoveSnapshotsUpdate {
public:
	RemoveSnapshotsUpdate();
	RemoveSnapshotsUpdate(const RemoveSnapshotsUpdate &) = delete;
	RemoveSnapshotsUpdate &operator=(const RemoveSnapshotsUpdate &) = delete;
	RemoveSnapshotsUpdate(RemoveSnapshotsUpdate &&) = default;
	RemoveSnapshotsUpdate &operator=(RemoveSnapshotsUpdate &&) = default;

public:
	// Deserialization
	static RemoveSnapshotsUpdate FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	RemoveSnapshotsUpdate Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	BaseUpdate base_update;
	vector<int64_t> snapshot_ids;
};

} // namespace rest_api_objects
} // namespace duckdb
