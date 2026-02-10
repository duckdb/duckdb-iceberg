
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

using namespace duckdb_yyjson;

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
		static Object2 FromJSON(yyjson_val *obj);

	public:
		string TryFromJSON(yyjson_val *obj);

	public:
		string operation;
		case_insensitive_map_t<string> additional_properties;
	};

public:
	static Snapshot FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int64_t snapshot_id;
	int64_t timestamp_ms;
	string manifest_list;
	Object2 summary;
	int64_t parent_snapshot_id;
	bool has_parent_snapshot_id = false;
	int64_t sequence_number;
	bool has_sequence_number = false;
	int64_t first_row_id;
	bool has_first_row_id = false;
	int64_t added_rows;
	bool has_added_rows = false;
	int32_t schema_id;
	bool has_schema_id = false;
};

} // namespace rest_api_objects
} // namespace duckdb
