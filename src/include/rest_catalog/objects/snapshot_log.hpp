
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SnapshotLog {
public:
	SnapshotLog();
	SnapshotLog(const SnapshotLog &) = delete;
	SnapshotLog &operator=(const SnapshotLog &) = delete;
	SnapshotLog(SnapshotLog &&) = default;
	SnapshotLog &operator=(SnapshotLog &&) = default;
	class Object3 {
	public:
		Object3();
		Object3(const Object3 &) = delete;
		Object3 &operator=(const Object3 &) = delete;
		Object3(Object3 &&) = default;
		Object3 &operator=(Object3 &&) = default;

	public:
		static Object3 FromJSON(yyjson_val *obj);

	public:
		string TryFromJSON(yyjson_val *obj);

	public:
		int64_t snapshot_id;
		int64_t timestamp_ms;
	};

public:
	static SnapshotLog FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	vector<Object3> value;
};

} // namespace rest_api_objects
} // namespace duckdb
