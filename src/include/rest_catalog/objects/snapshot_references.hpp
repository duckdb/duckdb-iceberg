
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/objects/snapshot_reference.hpp"

namespace duckdb {
namespace rest_api_objects {

class SnapshotReferences {
public:
	SnapshotReferences();
	SnapshotReferences(const SnapshotReferences &) = delete;
	SnapshotReferences &operator=(const SnapshotReferences &) = delete;
	SnapshotReferences(SnapshotReferences &&) = default;
	SnapshotReferences &operator=(SnapshotReferences &&) = default;

public:
	// Deserialization
	static SnapshotReferences FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SnapshotReferences Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	unordered_map<string, SnapshotReference> additional_properties;
};

} // namespace rest_api_objects
} // namespace duckdb
