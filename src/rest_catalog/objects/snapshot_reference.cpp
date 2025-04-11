
#include "rest_catalog/objects/snapshot_reference.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SnapshotReference::SnapshotReference() {
}

SnapshotReference SnapshotReference::FromJSON(yyjson_val *obj) {
	SnapshotReference res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SnapshotReference::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "SnapshotReference required property 'type' is missing";
	} else {
		type = yyjson_get_str(type_val);
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "SnapshotReference required property 'snapshot-id' is missing";
	} else {
		snapshot_id = yyjson_get_sint(snapshot_id_val);
	}
	auto max_ref_age_ms_val = yyjson_obj_get(obj, "max-ref-age-ms");
	if (max_ref_age_ms_val) {
		has_max_ref_age_ms = true;
		max_ref_age_ms = yyjson_get_sint(max_ref_age_ms_val);
	}
	auto max_snapshot_age_ms_val = yyjson_obj_get(obj, "max-snapshot-age-ms");
	if (max_snapshot_age_ms_val) {
		has_max_snapshot_age_ms = true;
		max_snapshot_age_ms = yyjson_get_sint(max_snapshot_age_ms_val);
	}
	auto min_snapshots_to_keep_val = yyjson_obj_get(obj, "min-snapshots-to-keep");
	if (min_snapshots_to_keep_val) {
		has_min_snapshots_to_keep = true;
		min_snapshots_to_keep = yyjson_get_sint(min_snapshots_to_keep_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
