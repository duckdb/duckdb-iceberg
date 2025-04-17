
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/base_update.hpp"

using namespace duckdb_yyjson;

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
	static RemoveSnapshotsUpdate FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	BaseUpdate base_update;
	vector<int64_t> snapshot_ids;
	string action;
	bool has_action = false;
};

} // namespace rest_api_objects
} // namespace duckdb
