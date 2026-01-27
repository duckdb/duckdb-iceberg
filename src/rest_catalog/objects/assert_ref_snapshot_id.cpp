
#include "rest_catalog/objects/assert_ref_snapshot_id.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

AssertRefSnapshotId::AssertRefSnapshotId() {
}

AssertRefSnapshotId AssertRefSnapshotId::FromJSON(yyjson_val *obj) {
	AssertRefSnapshotId res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string AssertRefSnapshotId::TryFromJSON(yyjson_val *obj) {
	string error;
	auto type_val = yyjson_obj_get(obj, "type");
	if (!type_val) {
		return "AssertRefSnapshotId required property 'type' is missing";
	} else {
		error = type.TryFromJSON(type_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto ref_val = yyjson_obj_get(obj, "ref");
	if (!ref_val) {
		return "AssertRefSnapshotId required property 'ref' is missing";
	} else {
		if (yyjson_is_str(ref_val)) {
			ref = yyjson_get_str(ref_val);
		} else {
			return StringUtil::Format("AssertRefSnapshotId property 'ref' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(ref_val));
		}
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (snapshot_id_val) {
		has_snapshot_id = true;
		if (yyjson_is_null(snapshot_id_val)) {
			//! do nothing, property is explicitly nullable
			has_snapshot_id = false;
		} else if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else if (yyjson_is_uint(snapshot_id_val)) {
			snapshot_id = yyjson_get_uint(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "AssertRefSnapshotId property 'snapshot_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(snapshot_id_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
