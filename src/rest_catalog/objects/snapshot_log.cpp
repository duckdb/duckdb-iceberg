
#include "rest_catalog/objects/snapshot_log.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

SnapshotLog::SnapshotLog() {
}
SnapshotLog::Object3::Object3() {
}

SnapshotLog::Object3 SnapshotLog::Object3::FromJSON(yyjson_val *obj) {
	Object3 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SnapshotLog::Object3::TryFromJSON(yyjson_val *obj) {
	string error;
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "Object3 required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_int(snapshot_id_val)) {
			snapshot_id = yyjson_get_int(snapshot_id_val);
		} else {
			return StringUtil::Format("Object3 property 'snapshot_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(snapshot_id_val));
		}
	}
	auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
	if (!timestamp_ms_val) {
		return "Object3 required property 'timestamp-ms' is missing";
	} else {
		if (yyjson_is_int(timestamp_ms_val)) {
			timestamp_ms = yyjson_get_int(timestamp_ms_val);
		} else {
			return StringUtil::Format("Object3 property 'timestamp_ms' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(timestamp_ms_val));
		}
	}
	return string();
}

SnapshotLog SnapshotLog::FromJSON(yyjson_val *obj) {
	SnapshotLog res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string SnapshotLog::TryFromJSON(yyjson_val *obj) {
	string error;
	if (yyjson_is_arr(obj)) {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(obj, idx, max, val) {
			Object3 tmp;
			error = tmp.TryFromJSON(val);
			if (!error.empty()) {
				return error;
			}
			value.emplace_back(std::move(tmp));
		}
	} else {
		return StringUtil::Format("SnapshotLog property 'value' is not of type 'array', found '%s' instead",
		                          yyjson_get_type_desc(obj));
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
