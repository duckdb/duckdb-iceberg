
#include "rest_catalog/objects/scan_report.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

ScanReport::ScanReport() {
}

ScanReport ScanReport::FromJSON(yyjson_val *obj) {
	ScanReport res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string ScanReport::TryFromJSON(yyjson_val *obj) {
	string error;
	auto table_name_val = yyjson_obj_get(obj, "table-name");
	if (!table_name_val) {
		return "ScanReport required property 'table-name' is missing";
	} else {
		table_name = yyjson_get_str(table_name_val);
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "ScanReport required property 'snapshot-id' is missing";
	} else {
		snapshot_id = yyjson_get_sint(snapshot_id_val);
	}
	auto filter_val = yyjson_obj_get(obj, "filter");
	if (!filter_val) {
		return "ScanReport required property 'filter' is missing";
	} else {
		filter = make_uniq<Expression>();
		error = filter->TryFromJSON(filter_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto schema_id_val = yyjson_obj_get(obj, "schema-id");
	if (!schema_id_val) {
		return "ScanReport required property 'schema-id' is missing";
	} else {
		schema_id = yyjson_get_sint(schema_id_val);
	}
	auto projected_field_ids_val = yyjson_obj_get(obj, "projected-field-ids");
	if (!projected_field_ids_val) {
		return "ScanReport required property 'projected-field-ids' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(projected_field_ids_val, idx, max, val) {
			auto tmp = yyjson_get_sint(val);
			projected_field_ids.emplace_back(std::move(tmp));
		}
	}
	auto projected_field_names_val = yyjson_obj_get(obj, "projected-field-names");
	if (!projected_field_names_val) {
		return "ScanReport required property 'projected-field-names' is missing";
	} else {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(projected_field_names_val, idx, max, val) {
			auto tmp = yyjson_get_str(val);
			projected_field_names.emplace_back(std::move(tmp));
		}
	}
	auto metrics_val = yyjson_obj_get(obj, "metrics");
	if (!metrics_val) {
		return "ScanReport required property 'metrics' is missing";
	} else {
		error = metrics.TryFromJSON(metrics_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto metadata_val = yyjson_obj_get(obj, "metadata");
	if (metadata_val) {
		has_metadata = true;
		metadata = parse_object_of_strings(metadata_val);
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
