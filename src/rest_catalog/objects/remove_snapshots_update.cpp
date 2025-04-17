
#include "rest_catalog/objects/remove_snapshots_update.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

RemoveSnapshotsUpdate::RemoveSnapshotsUpdate() {
}

RemoveSnapshotsUpdate RemoveSnapshotsUpdate::FromJSON(yyjson_val *obj) {
	RemoveSnapshotsUpdate res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string RemoveSnapshotsUpdate::TryFromJSON(yyjson_val *obj) {
	string error;
	error = base_update.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto snapshot_ids_val = yyjson_obj_get(obj, "snapshot-ids");
	if (!snapshot_ids_val) {
		return "RemoveSnapshotsUpdate required property 'snapshot-ids' is missing";
	} else {
		if (yyjson_is_arr(snapshot_ids_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(snapshot_ids_val, idx, max, val) {
				int64_t tmp;
				if (yyjson_is_int(val)) {
					tmp = yyjson_get_int(val);
				} else {
					return StringUtil::Format(
					    "RemoveSnapshotsUpdate property 'tmp' is not of type 'integer', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				snapshot_ids.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "RemoveSnapshotsUpdate property 'snapshot_ids' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(snapshot_ids_val));
		}
	}
	auto action_val = yyjson_obj_get(obj, "action");
	if (action_val) {
		has_action = true;
		if (yyjson_is_str(action_val)) {
			action = yyjson_get_str(action_val);
		} else {
			return StringUtil::Format(
			    "RemoveSnapshotsUpdate property 'action' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(action_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
