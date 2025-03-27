#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/field_name.hpp"
#include "rest_catalog/objects/expression.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PlanTableScanRequest {
public:
	static PlanTableScanRequest FromJSON(yyjson_val *obj) {
		PlanTableScanRequest result;

		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		}

		auto select_val = yyjson_obj_get(obj, "select");
		if (select_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(select_val, idx, max, val) {
				result.select.push_back(FieldName::FromJSON(val));
			}
		}

		auto filter_val = yyjson_obj_get(obj, "filter");
		if (filter_val) {
			result.filter = Expression::FromJSON(filter_val);
		}

		auto case_sensitive_val = yyjson_obj_get(obj, "case-sensitive");
		if (case_sensitive_val) {
			result.case_sensitive = yyjson_get_bool(case_sensitive_val);
		}

		auto use_snapshot_schema_val = yyjson_obj_get(obj, "use-snapshot-schema");
		if (use_snapshot_schema_val) {
			result.use_snapshot_schema = yyjson_get_bool(use_snapshot_schema_val);
		}

		auto start_snapshot_id_val = yyjson_obj_get(obj, "start-snapshot-id");
		if (start_snapshot_id_val) {
			result.start_snapshot_id = yyjson_get_sint(start_snapshot_id_val);
		}

		auto end_snapshot_id_val = yyjson_obj_get(obj, "end-snapshot-id");
		if (end_snapshot_id_val) {
			result.end_snapshot_id = yyjson_get_sint(end_snapshot_id_val);
		}

		auto stats_fields_val = yyjson_obj_get(obj, "stats-fields");
		if (stats_fields_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(stats_fields_val, idx, max, val) {
				result.stats_fields.push_back(FieldName::FromJSON(val));
			}
		}

		return result;
	}

public:
	int64_t snapshot_id;
	vector<FieldName> select;
	Expression filter;
	bool case_sensitive;
	bool use_snapshot_schema;
	int64_t start_snapshot_id;
	int64_t end_snapshot_id;
	vector<FieldName> stats_fields;
};
} // namespace rest_api_objects
} // namespace duckdb