
#include "rest_catalog/objects/snapshot_log.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

SnapshotLog::Object3 SnapshotLog::Object3::Copy() const {
	Object3 res;
	res.snapshot_id = snapshot_id;
	res.timestamp_ms = timestamp_ms;
	return res;
}

string SnapshotLog::Object3::TryFromJSON(yyjson_val *obj) {
	string error;
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "Object3 required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else if (yyjson_is_uint(snapshot_id_val)) {
			snapshot_id = yyjson_get_uint(snapshot_id_val);
		} else {
			return StringUtil::Format("Object3 property 'snapshot_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(snapshot_id_val));
		}
	}
	auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
	if (!timestamp_ms_val) {
		return "Object3 required property 'timestamp-ms' is missing";
	} else {
		if (yyjson_is_sint(timestamp_ms_val)) {
			timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		} else if (yyjson_is_uint(timestamp_ms_val)) {
			timestamp_ms = yyjson_get_uint(timestamp_ms_val);
		} else {
			return StringUtil::Format("Object3 property 'timestamp_ms' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(timestamp_ms_val));
		}
	}
	return "";
}

void SnapshotLog::Object3::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: snapshot-id
	yyjson_mut_obj_add_sint(doc, obj, "snapshot-id", snapshot_id);

	// Serialize: timestamp-ms
	yyjson_mut_obj_add_sint(doc, obj, "timestamp-ms", timestamp_ms);
}

yyjson_mut_val *SnapshotLog::Object3::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

SnapshotLog SnapshotLog::FromJSON(yyjson_val *obj) {
	SnapshotLog res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

SnapshotLog SnapshotLog::Copy() const {
	SnapshotLog res;
	res.value.reserve(value.size());
	for (auto &item : value) {
		res.value.emplace_back(item.Copy());
	}
	return res;
}

string SnapshotLog::TryFromJSON(yyjson_val *obj) {
	string error;
	if (yyjson_is_arr(obj)) {
		size_t value_idx, value_max;
		yyjson_val *value_item_val;
		yyjson_arr_foreach(obj, value_idx, value_max, value_item_val) {
			Object3 value_item;
			error = value_item.TryFromJSON(value_item_val);
			if (!error.empty()) {
				return error;
			}
			value.emplace_back(std::move(value_item));
		}
	} else {
		return StringUtil::Format("SnapshotLog property 'value' is not of type 'array', found '%s' instead",
		                          yyjson_get_type_desc(obj));
	}
	return "";
}

yyjson_mut_val *SnapshotLog::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *arr = yyjson_mut_arr(doc);
	for (const auto &item : value) {
		yyjson_mut_arr_append(arr, item.ToJSON(doc));
	}
	return arr;
}

} // namespace rest_api_objects
} // namespace duckdb
