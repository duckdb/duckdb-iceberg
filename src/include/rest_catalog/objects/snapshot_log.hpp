
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class SnapshotLog {
public:
	SnapshotLog();
	SnapshotLog(const SnapshotLog &) = delete;
	SnapshotLog &operator=(const SnapshotLog &) = delete;
	SnapshotLog(SnapshotLog &&) = default;
	SnapshotLog &operator=(SnapshotLog &&) = default;
	class Object7 {
	public:
		Object7();
		Object7(const Object7 &) = delete;
		Object7 &operator=(const Object7 &) = delete;
		Object7(Object7 &&) = default;
		Object7 &operator=(Object7 &&) = default;

	public:
		// Deserialization
		static Object7 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object7 Copy() const;

		// Serialization
		void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		int64_t snapshot_id;
		int64_t timestamp_ms;
	};

public:
	// Deserialization
	static SnapshotLog FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	SnapshotLog Copy() const;

	// Serialization
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	vector<Object7> value;
};

} // namespace rest_api_objects
} // namespace duckdb
