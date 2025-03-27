#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SnapshotReference {
public:
	static SnapshotReference FromJSON(yyjson_val *obj) {
		SnapshotReference result;

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		else {
			throw IOException("SnapshotReference required property 'type' is missing");
		}

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		}
		else {
			throw IOException("SnapshotReference required property 'snapshot-id' is missing");
		}

		auto max_ref_age_ms_val = yyjson_obj_get(obj, "max-ref-age-ms");
		if (max_ref_age_ms_val) {
			result.max_ref_age_ms = yyjson_get_sint(max_ref_age_ms_val);
		}

		auto max_snapshot_age_ms_val = yyjson_obj_get(obj, "max-snapshot-age-ms");
		if (max_snapshot_age_ms_val) {
			result.max_snapshot_age_ms = yyjson_get_sint(max_snapshot_age_ms_val);
		}

		auto min_snapshots_to_keep_val = yyjson_obj_get(obj, "min-snapshots-to-keep");
		if (min_snapshots_to_keep_val) {
			result.min_snapshots_to_keep = yyjson_get_sint(min_snapshots_to_keep_val);
		}

		return result;
	}

public:
	string type;
	int64_t snapshot_id;
	int64_t max_ref_age_ms;
	int64_t max_snapshot_age_ms;
	int64_t min_snapshots_to_keep;
};
} // namespace rest_api_objects
} // namespace duckdb