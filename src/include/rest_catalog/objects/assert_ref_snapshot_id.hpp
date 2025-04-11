
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/table_requirement.hpp"

using namespace duckdb_yyjson;

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
	static AssertRefSnapshotId FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	TableRequirement table_requirement;
	string ref;
	int64_t snapshot_id;
	string type;
	bool has_type = false;
};

} // namespace rest_api_objects
} // namespace duckdb
