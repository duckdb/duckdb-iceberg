#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Snapshot {
public:
	static Snapshot FromJSON(yyjson_val *obj) {
		Snapshot result;

		auto manifest_list_val = yyjson_obj_get(obj, "manifest-list");
		if (manifest_list_val) {
			result.manifest_list = yyjson_get_str(manifest_list_val);
		}
		else {
			throw IOException("Snapshot required property 'manifest-list' is missing");
		}

		auto parent_snapshot_id_val = yyjson_obj_get(obj, "parent-snapshot-id");
		if (parent_snapshot_id_val) {
			result.parent_snapshot_id = yyjson_get_sint(parent_snapshot_id_val);
		}

		auto schema_id_val = yyjson_obj_get(obj, "schema-id");
		if (schema_id_val) {
			result.schema_id = yyjson_get_sint(schema_id_val);
		}

		auto sequence_number_val = yyjson_obj_get(obj, "sequence-number");
		if (sequence_number_val) {
			result.sequence_number = yyjson_get_sint(sequence_number_val);
		}

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		}
		else {
			throw IOException("Snapshot required property 'snapshot-id' is missing");
		}

		auto summary_val = yyjson_obj_get(obj, "summary");
		if (summary_val) {
			result.summary = parse_object_of_strings(summary_val);
		}
		else {
			throw IOException("Snapshot required property 'summary' is missing");
		}

		auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
		if (timestamp_ms_val) {
			result.timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		}
		else {
			throw IOException("Snapshot required property 'timestamp-ms' is missing");
		}

		return result;
	}

public:
	string manifest_list;
	int64_t parent_snapshot_id;
	int64_t schema_id;
	int64_t sequence_number;
	int64_t snapshot_id;
	case_insensitive_map_t<string> summary;
	int64_t timestamp_ms;
};
} // namespace rest_api_objects
} // namespace duckdb