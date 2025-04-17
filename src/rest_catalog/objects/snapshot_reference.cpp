
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
		if (yyjson_is_str(type_val)) {
			type = yyjson_get_str(type_val);
		} else {
			return StringUtil::Format("SnapshotReference property 'type' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(type_val));
		}
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "SnapshotReference required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_int(snapshot_id_val)) {
			snapshot_id = yyjson_get_int(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "SnapshotReference property 'snapshot_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(snapshot_id_val));
		}
	}
	auto max_ref_age_ms_val = yyjson_obj_get(obj, "max-ref-age-ms");
	if (max_ref_age_ms_val) {
		has_max_ref_age_ms = true;
		if (yyjson_is_int(max_ref_age_ms_val)) {
			max_ref_age_ms = yyjson_get_int(max_ref_age_ms_val);
		} else {
			return StringUtil::Format(
			    "SnapshotReference property 'max_ref_age_ms' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(max_ref_age_ms_val));
		}
	}
	auto max_snapshot_age_ms_val = yyjson_obj_get(obj, "max-snapshot-age-ms");
	if (max_snapshot_age_ms_val) {
		has_max_snapshot_age_ms = true;
		if (yyjson_is_int(max_snapshot_age_ms_val)) {
			max_snapshot_age_ms = yyjson_get_int(max_snapshot_age_ms_val);
		} else {
			return StringUtil::Format(
			    "SnapshotReference property 'max_snapshot_age_ms' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(max_snapshot_age_ms_val));
		}
	}
	auto min_snapshots_to_keep_val = yyjson_obj_get(obj, "min-snapshots-to-keep");
	if (min_snapshots_to_keep_val) {
		has_min_snapshots_to_keep = true;
		if (yyjson_is_int(min_snapshots_to_keep_val)) {
			min_snapshots_to_keep = yyjson_get_int(min_snapshots_to_keep_val);
		} else {
			return StringUtil::Format(
			    "SnapshotReference property 'min_snapshots_to_keep' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(min_snapshots_to_keep_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
