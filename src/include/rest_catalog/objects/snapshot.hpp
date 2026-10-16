
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
	class Object6 {
	public:
		Object6();
		Object6(const Object6 &) = delete;
		Object6 &operator=(const Object6 &) = delete;
		Object6(Object6 &&) = default;
		Object6 &operator=(Object6 &&) = default;

	public:
		// Deserialization
		static Object6 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object6 Copy() const;

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
	Object6 summary;
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
