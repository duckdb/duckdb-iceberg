
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class SnapshotReference {
public:
	SnapshotReference();
	SnapshotReference(const SnapshotReference &) = delete;
	SnapshotReference &operator=(const SnapshotReference &) = delete;
	SnapshotReference(SnapshotReference &&) = default;
	SnapshotReference &operator=(SnapshotReference &&) = default;

public:
	// Deserialization
	static SnapshotReference FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SnapshotReference Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	int64_t snapshot_id;
	optional<int64_t> max_ref_age_ms;
	optional<int64_t> max_snapshot_age_ms;
	optional<int32_t> min_snapshots_to_keep;
};

} // namespace rest_api_objects
} // namespace duckdb
