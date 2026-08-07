
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
namespace rest_api_objects {

class Snapshot {
public:
	Snapshot();
	Snapshot(const Snapshot &) = delete;
	Snapshot &operator=(const Snapshot &) = delete;
	Snapshot(Snapshot &&) = default;
	Snapshot &operator=(Snapshot &&) = default;
	class Object2 {
	public:
		Object2();
		Object2(const Object2 &) = delete;
		Object2 &operator=(const Object2 &) = delete;
		Object2(Object2 &&) = default;
		Object2 &operator=(Object2 &&) = default;

	public:
		// Deserialization
		static Object2 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object2 Copy() const;

		// Serialization
		void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		string operation;
		case_insensitive_map_t<string> additional_properties;
	};

public:
	// Deserialization
	static Snapshot FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Snapshot Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int64_t snapshot_id;
	int64_t timestamp_ms;
	Object2 summary;
	optional<int64_t> parent_snapshot_id;
	optional<int64_t> sequence_number;
	optional<string> manifest_list;
	optional<vector<string>> manifests;
	optional<int64_t> first_row_id;
	optional<int64_t> added_rows;
	optional<int32_t> schema_id;
};

} // namespace rest_api_objects
} // namespace duckdb
