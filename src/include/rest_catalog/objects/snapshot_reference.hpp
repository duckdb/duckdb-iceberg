
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class SnapshotReference {
public:
	SnapshotReference() {
	}

public:
	static SnapshotReference FromJSON(yyjson_val *obj) {
		SnapshotReference res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto type_val = yyjson_obj_get(obj, "type");
		if (!type_val) {
			return "SnapshotReference required property 'type' is missing";
		} else {
			type = yyjson_get_str(type_val);
		}

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot_id");
		if (!snapshot_id_val) {
			return "SnapshotReference required property 'snapshot_id' is missing";
		} else {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		}

		auto max_ref_age_ms_val = yyjson_obj_get(obj, "max_ref_age_ms");
		if (max_ref_age_ms_val) {
			max_ref_age_ms = yyjson_get_sint(max_ref_age_ms_val);
		}

		auto max_snapshot_age_ms_val = yyjson_obj_get(obj, "max_snapshot_age_ms");
		if (max_snapshot_age_ms_val) {
			max_snapshot_age_ms = yyjson_get_sint(max_snapshot_age_ms_val);
		}

		auto min_snapshots_to_keep_val = yyjson_obj_get(obj, "min_snapshots_to_keep");
		if (min_snapshots_to_keep_val) {
			min_snapshots_to_keep = yyjson_get_sint(min_snapshots_to_keep_val);
		}

		return string();
	}

public:
public:
	int64_t max_ref_age_ms;
	int64_t max_snapshot_age_ms;
	int64_t min_snapshots_to_keep;
	int64_t snapshot_id;
	string type;
};

} // namespace rest_api_objects
} // namespace duckdb
