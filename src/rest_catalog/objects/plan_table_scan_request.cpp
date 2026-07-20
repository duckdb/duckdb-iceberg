
#include "rest_catalog/objects/plan_table_scan_request.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PlanTableScanRequest::PlanTableScanRequest() {
}

PlanTableScanRequest PlanTableScanRequest::FromJSON(yyjson_val *obj) {
	PlanTableScanRequest res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

PlanTableScanRequest PlanTableScanRequest::Copy() const {
	PlanTableScanRequest res;
	if (snapshot_id.has_value()) {
		res.snapshot_id.emplace();
		(*res.snapshot_id) = (*snapshot_id);
	}
	if (select.has_value()) {
		res.select.emplace();
		(*res.select).reserve((*select).size());
		for (auto &item : (*select)) {
			(*res.select).emplace_back(item.Copy());
		}
	}
	if (filter != nullptr) {
		res.filter = filter ? make_uniq<Expression>(filter->Copy()) : nullptr;
	}
	if (min_rows_requested.has_value()) {
		res.min_rows_requested.emplace();
		(*res.min_rows_requested) = (*min_rows_requested);
	}
	if (case_sensitive.has_value()) {
		res.case_sensitive.emplace();
		(*res.case_sensitive) = (*case_sensitive);
	}
	if (use_snapshot_schema.has_value()) {
		res.use_snapshot_schema.emplace();
		(*res.use_snapshot_schema) = (*use_snapshot_schema);
	}
	if (start_snapshot_id.has_value()) {
		res.start_snapshot_id.emplace();
		(*res.start_snapshot_id) = (*start_snapshot_id);
	}
	if (end_snapshot_id.has_value()) {
		res.end_snapshot_id.emplace();
		(*res.end_snapshot_id) = (*end_snapshot_id);
	}
	if (stats_fields.has_value()) {
		res.stats_fields.emplace();
		(*res.stats_fields).reserve((*stats_fields).size());
		for (auto &item : (*stats_fields)) {
			(*res.stats_fields).emplace_back(item.Copy());
		}
	}
	return res;
}

string PlanTableScanRequest::TryFromJSON(yyjson_val *obj) {
	string error;
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (snapshot_id_val) {
		int64_t snapshot_id_tmp;
		if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id_tmp = yyjson_get_sint(snapshot_id_val);
		} else if (yyjson_is_uint(snapshot_id_val)) {
			snapshot_id_tmp = yyjson_get_uint(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'snapshot_id_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(snapshot_id_val));
		}
		snapshot_id = std::move(snapshot_id_tmp);
	}
	auto select_val = yyjson_obj_get(obj, "select");
	if (select_val) {
		vector<FieldName> select_tmp;
		if (yyjson_is_arr(select_val)) {
			size_t select_tmp_idx, select_tmp_max;
			yyjson_val *select_tmp_item_val;
			yyjson_arr_foreach(select_val, select_tmp_idx, select_tmp_max, select_tmp_item_val) {
				FieldName select_tmp_item;
				error = select_tmp_item.TryFromJSON(select_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				select_tmp.emplace_back(std::move(select_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'select_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(select_val));
		}
		select = std::move(select_tmp);
	}
	auto filter_val = yyjson_obj_get(obj, "filter");
	if (filter_val) {
		filter = make_uniq<Expression>();
		error = filter->TryFromJSON(filter_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto min_rows_requested_val = yyjson_obj_get(obj, "min-rows-requested");
	if (min_rows_requested_val) {
		int64_t min_rows_requested_tmp;
		if (yyjson_is_sint(min_rows_requested_val)) {
			min_rows_requested_tmp = yyjson_get_sint(min_rows_requested_val);
		} else if (yyjson_is_uint(min_rows_requested_val)) {
			min_rows_requested_tmp = yyjson_get_uint(min_rows_requested_val);
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'min_rows_requested_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(min_rows_requested_val));
		}
		min_rows_requested = std::move(min_rows_requested_tmp);
	}
	auto case_sensitive_val = yyjson_obj_get(obj, "case-sensitive");
	if (case_sensitive_val) {
		bool case_sensitive_tmp;
		if (yyjson_is_bool(case_sensitive_val)) {
			case_sensitive_tmp = yyjson_get_bool(case_sensitive_val);
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'case_sensitive_tmp' is not of type 'boolean', found '%s' instead",
			    yyjson_get_type_desc(case_sensitive_val));
		}
		case_sensitive = std::move(case_sensitive_tmp);
	}
	auto use_snapshot_schema_val = yyjson_obj_get(obj, "use-snapshot-schema");
	if (use_snapshot_schema_val) {
		bool use_snapshot_schema_tmp;
		if (yyjson_is_bool(use_snapshot_schema_val)) {
			use_snapshot_schema_tmp = yyjson_get_bool(use_snapshot_schema_val);
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'use_snapshot_schema_tmp' is not of type 'boolean', found '%s' instead",
			    yyjson_get_type_desc(use_snapshot_schema_val));
		}
		use_snapshot_schema = std::move(use_snapshot_schema_tmp);
	}
	auto start_snapshot_id_val = yyjson_obj_get(obj, "start-snapshot-id");
	if (start_snapshot_id_val) {
		int64_t start_snapshot_id_tmp;
		if (yyjson_is_sint(start_snapshot_id_val)) {
			start_snapshot_id_tmp = yyjson_get_sint(start_snapshot_id_val);
		} else if (yyjson_is_uint(start_snapshot_id_val)) {
			start_snapshot_id_tmp = yyjson_get_uint(start_snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'start_snapshot_id_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(start_snapshot_id_val));
		}
		start_snapshot_id = std::move(start_snapshot_id_tmp);
	}
	auto end_snapshot_id_val = yyjson_obj_get(obj, "end-snapshot-id");
	if (end_snapshot_id_val) {
		int64_t end_snapshot_id_tmp;
		if (yyjson_is_sint(end_snapshot_id_val)) {
			end_snapshot_id_tmp = yyjson_get_sint(end_snapshot_id_val);
		} else if (yyjson_is_uint(end_snapshot_id_val)) {
			end_snapshot_id_tmp = yyjson_get_uint(end_snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'end_snapshot_id_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(end_snapshot_id_val));
		}
		end_snapshot_id = std::move(end_snapshot_id_tmp);
	}
	auto stats_fields_val = yyjson_obj_get(obj, "stats-fields");
	if (stats_fields_val) {
		vector<FieldName> stats_fields_tmp;
		if (yyjson_is_arr(stats_fields_val)) {
			size_t stats_fields_tmp_idx, stats_fields_tmp_max;
			yyjson_val *stats_fields_tmp_item_val;
			yyjson_arr_foreach(stats_fields_val, stats_fields_tmp_idx, stats_fields_tmp_max,
			                   stats_fields_tmp_item_val) {
				FieldName stats_fields_tmp_item;
				error = stats_fields_tmp_item.TryFromJSON(stats_fields_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				stats_fields_tmp.emplace_back(std::move(stats_fields_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "PlanTableScanRequest property 'stats_fields_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(stats_fields_val));
		}
		stats_fields = std::move(stats_fields_tmp);
	}
	return "";
}

void PlanTableScanRequest::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: snapshot-id
	if (snapshot_id.has_value()) {
		auto &snapshot_id_value = *snapshot_id;
		yyjson_mut_obj_add_sint(doc, obj, "snapshot-id", snapshot_id_value);
	}

	// Serialize: select
	if (select.has_value()) {
		auto &select_value = *select;
		yyjson_mut_val *select_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : select_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(select_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "select", select_value_arr);
	}

	// Serialize: filter
	if (filter != nullptr) {
		yyjson_mut_val *filter_val = filter->ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "filter", filter_val);
	}

	// Serialize: min-rows-requested
	if (min_rows_requested.has_value()) {
		auto &min_rows_requested_value = *min_rows_requested;
		yyjson_mut_obj_add_sint(doc, obj, "min-rows-requested", min_rows_requested_value);
	}

	// Serialize: case-sensitive
	if (case_sensitive.has_value()) {
		auto &case_sensitive_value = *case_sensitive;
		yyjson_mut_obj_add_bool(doc, obj, "case-sensitive", case_sensitive_value);
	}

	// Serialize: use-snapshot-schema
	if (use_snapshot_schema.has_value()) {
		auto &use_snapshot_schema_value = *use_snapshot_schema;
		yyjson_mut_obj_add_bool(doc, obj, "use-snapshot-schema", use_snapshot_schema_value);
	}

	// Serialize: start-snapshot-id
	if (start_snapshot_id.has_value()) {
		auto &start_snapshot_id_value = *start_snapshot_id;
		yyjson_mut_obj_add_sint(doc, obj, "start-snapshot-id", start_snapshot_id_value);
	}

	// Serialize: end-snapshot-id
	if (end_snapshot_id.has_value()) {
		auto &end_snapshot_id_value = *end_snapshot_id;
		yyjson_mut_obj_add_sint(doc, obj, "end-snapshot-id", end_snapshot_id_value);
	}

	// Serialize: stats-fields
	if (stats_fields.has_value()) {
		auto &stats_fields_value = *stats_fields;
		yyjson_mut_val *stats_fields_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : stats_fields_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(stats_fields_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "stats-fields", stats_fields_value_arr);
	}
}

yyjson_mut_val *PlanTableScanRequest::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
