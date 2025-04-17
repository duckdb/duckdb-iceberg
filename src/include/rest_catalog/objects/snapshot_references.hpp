
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/snapshot_reference.hpp"

using namespace duckdb_yyjson;

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
	static SnapshotReferences FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	case_insensitive_map_t<SnapshotReference> additional_properties;
};

} // namespace rest_api_objects
} // namespace duckdb
