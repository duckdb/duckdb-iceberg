
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class AssertRefSnapshotId {
public:
	AssertRefSnapshotId();
	AssertRefSnapshotId(const AssertRefSnapshotId &) = delete;
	AssertRefSnapshotId &operator=(const AssertRefSnapshotId &) = delete;
	AssertRefSnapshotId(AssertRefSnapshotId &&) = default;
	AssertRefSnapshotId &operator=(AssertRefSnapshotId &&) = default;

public:
	// Deserialization
	static AssertRefSnapshotId FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	AssertRefSnapshotId Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	string type;
	string ref;
	optional<int64_t> snapshot_id;
};

} // namespace rest_api_objects
} // namespace duckdb
