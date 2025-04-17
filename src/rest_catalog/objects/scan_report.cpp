
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
		if (yyjson_is_str(table_name_val)) {
			table_name = yyjson_get_str(table_name_val);
		} else {
			return StringUtil::Format("ScanReport property 'table_name' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(table_name_val));
		}
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "ScanReport required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_int(snapshot_id_val)) {
			snapshot_id = yyjson_get_int(snapshot_id_val);
		} else {
			return StringUtil::Format("ScanReport property 'snapshot_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(snapshot_id_val));
		}
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
		if (yyjson_is_int(schema_id_val)) {
			schema_id = yyjson_get_int(schema_id_val);
		} else {
			return StringUtil::Format("ScanReport property 'schema_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(schema_id_val));
		}
	}
	auto projected_field_ids_val = yyjson_obj_get(obj, "projected-field-ids");
	if (!projected_field_ids_val) {
		return "ScanReport required property 'projected-field-ids' is missing";
	} else {
		if (yyjson_is_arr(projected_field_ids_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(projected_field_ids_val, idx, max, val) {
				int64_t tmp;
				if (yyjson_is_int(val)) {
					tmp = yyjson_get_int(val);
				} else {
					return StringUtil::Format("ScanReport property 'tmp' is not of type 'integer', found '%s' instead",
					                          yyjson_get_type_desc(val));
				}
				projected_field_ids.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "ScanReport property 'projected_field_ids' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(projected_field_ids_val));
		}
	}
	auto projected_field_names_val = yyjson_obj_get(obj, "projected-field-names");
	if (!projected_field_names_val) {
		return "ScanReport required property 'projected-field-names' is missing";
	} else {
		if (yyjson_is_arr(projected_field_names_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(projected_field_names_val, idx, max, val) {
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format("ScanReport property 'tmp' is not of type 'string', found '%s' instead",
					                          yyjson_get_type_desc(val));
				}
				projected_field_names.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "ScanReport property 'projected_field_names' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(projected_field_names_val));
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
		if (yyjson_is_obj(metadata_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(metadata_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format("ScanReport property 'tmp' is not of type 'string', found '%s' instead",
					                          yyjson_get_type_desc(val));
				}
				metadata.emplace(key_str, std::move(tmp));
			}
		} else {
			return "ScanReport property 'metadata' is not of type 'object'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
