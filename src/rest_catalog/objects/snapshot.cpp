
#include "rest_catalog/objects/snapshot.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

Snapshot::Snapshot() {
}
Snapshot::Object2::Object2() {
}

Snapshot::Object2 Snapshot::Object2::FromJSON(yyjson_val *obj) {
	Object2 res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string Snapshot::Object2::TryFromJSON(yyjson_val *obj) {
	string error;
	auto operation_val = yyjson_obj_get(obj, "operation");
	if (!operation_val) {
		return "Object2 required property 'operation' is missing";
	} else {
		operation = yyjson_get_str(operation_val);
	}
	case_insensitive_set_t handled_properties {"operation"};
	size_t idx, max;
	yyjson_val *key, *val;
	yyjson_obj_foreach(obj, idx, max, key, val) {
		auto key_str = yyjson_get_str(key);
		if (handled_properties.count(key_str)) {
			continue;
		}
		auto tmp = yyjson_get_str(val);
		additional_properties.emplace(key_str, std::move(tmp));
	}
	return string();
}

Snapshot Snapshot::FromJSON(yyjson_val *obj) {
	Snapshot res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string Snapshot::TryFromJSON(yyjson_val *obj) {
	string error;
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "Snapshot required property 'snapshot-id' is missing";
	} else {
		snapshot_id = yyjson_get_sint(snapshot_id_val);
	}
	auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
	if (!timestamp_ms_val) {
		return "Snapshot required property 'timestamp-ms' is missing";
	} else {
		timestamp_ms = yyjson_get_sint(timestamp_ms_val);
	}
	auto manifest_list_val = yyjson_obj_get(obj, "manifest-list");
	if (!manifest_list_val) {
		return "Snapshot required property 'manifest-list' is missing";
	} else {
		manifest_list = yyjson_get_str(manifest_list_val);
	}
	auto summary_val = yyjson_obj_get(obj, "summary");
	if (!summary_val) {
		return "Snapshot required property 'summary' is missing";
	} else {
		error = summary.TryFromJSON(summary_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto parent_snapshot_id_val = yyjson_obj_get(obj, "parent-snapshot-id");
	if (parent_snapshot_id_val) {
		has_parent_snapshot_id = true;
		parent_snapshot_id = yyjson_get_sint(parent_snapshot_id_val);
	}
	auto sequence_number_val = yyjson_obj_get(obj, "sequence-number");
	if (sequence_number_val) {
		has_sequence_number = true;
		sequence_number = yyjson_get_sint(sequence_number_val);
	}
	auto schema_id_val = yyjson_obj_get(obj, "schema-id");
	if (schema_id_val) {
		has_schema_id = true;
		schema_id = yyjson_get_sint(schema_id_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
