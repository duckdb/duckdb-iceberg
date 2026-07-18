
#include "rest_catalog/objects/scan_report.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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

ScanReport ScanReport::Copy() const {
	ScanReport res;
	res.table_name = table_name;
	res.snapshot_id = snapshot_id;
	res.filter = filter ? make_uniq<Expression>(filter->Copy()) : nullptr;
	res.schema_id = schema_id;
	res.projected_field_ids.reserve(projected_field_ids.size());
	for (auto &item : projected_field_ids) {
		res.projected_field_ids.emplace_back(item);
	}
	res.projected_field_names.reserve(projected_field_names.size());
	for (auto &item : projected_field_names) {
		res.projected_field_names.emplace_back(item);
	}
	res.metrics = metrics.Copy();
	if (metadata.has_value()) {
		res.metadata.emplace();
		for (auto &entry : (*metadata)) {
			(*res.metadata).emplace(entry.first, entry.second);
		}
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
		if (yyjson_is_sint(snapshot_id_val)) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else if (yyjson_is_uint(snapshot_id_val)) {
			snapshot_id = yyjson_get_uint(snapshot_id_val);
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
			size_t projected_field_ids_idx, projected_field_ids_max;
			yyjson_val *projected_field_ids_item_val;
			yyjson_arr_foreach(projected_field_ids_val, projected_field_ids_idx, projected_field_ids_max,
			                   projected_field_ids_item_val) {
				int32_t projected_field_ids_item;
				if (yyjson_is_int(projected_field_ids_item_val)) {
					projected_field_ids_item = yyjson_get_int(projected_field_ids_item_val);
				} else {
					return StringUtil::Format(
					    "ScanReport property 'projected_field_ids_item' is not of type 'integer', found '%s' instead",
					    yyjson_get_type_desc(projected_field_ids_item_val));
				}
				projected_field_ids.emplace_back(std::move(projected_field_ids_item));
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
			size_t projected_field_names_idx, projected_field_names_max;
			yyjson_val *projected_field_names_item_val;
			yyjson_arr_foreach(projected_field_names_val, projected_field_names_idx, projected_field_names_max,
			                   projected_field_names_item_val) {
				string projected_field_names_item;
				if (yyjson_is_str(projected_field_names_item_val)) {
					projected_field_names_item = yyjson_get_str(projected_field_names_item_val);
				} else {
					return StringUtil::Format(
					    "ScanReport property 'projected_field_names_item' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(projected_field_names_item_val));
				}
				projected_field_names.emplace_back(std::move(projected_field_names_item));
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
		case_insensitive_map_t<string> metadata_tmp;
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
				metadata_tmp.emplace(key_str, std::move(tmp));
			}
		} else {
			return "ScanReport property 'metadata_tmp' is not of type 'object'";
		}
		metadata = std::move(metadata_tmp);
	}
	return "";
}

void ScanReport::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: table-name
	yyjson_mut_obj_add_strcpy(doc, obj, "table-name", table_name.c_str());

	// Serialize: snapshot-id
	yyjson_mut_obj_add_sint(doc, obj, "snapshot-id", snapshot_id);

	// Serialize: filter
	yyjson_mut_val *filter_val = filter->ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "filter", filter_val);

	// Serialize: schema-id
	yyjson_mut_obj_add_int(doc, obj, "schema-id", schema_id);

	// Serialize: projected-field-ids
	yyjson_mut_val *projected_field_ids_arr = yyjson_mut_arr(doc);
	for (const auto &item : projected_field_ids) {
		yyjson_mut_val *item_val = yyjson_mut_int(doc, item);
		yyjson_mut_arr_append(projected_field_ids_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "projected-field-ids", projected_field_ids_arr);

	// Serialize: projected-field-names
	yyjson_mut_val *projected_field_names_arr = yyjson_mut_arr(doc);
	for (const auto &item : projected_field_names) {
		yyjson_mut_val *item_val = yyjson_mut_str(doc, item.c_str());
		yyjson_mut_arr_append(projected_field_names_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "projected-field-names", projected_field_names_arr);

	// Serialize: metrics
	yyjson_mut_val *metrics_val = metrics.ToJSON(doc);
	yyjson_mut_obj_add_val(doc, obj, "metrics", metrics_val);

	// Serialize: metadata
	if (metadata.has_value()) {
		auto &metadata_value = *metadata;
		yyjson_mut_val *metadata_value_obj = yyjson_mut_obj(doc);
		for (const auto &it : metadata_value) {
			auto &key = it.first;
			auto &value = it.second;
			auto key_ptr = unsafe_yyjson_mut_strncpy(doc, key.c_str(), strlen(key.c_str()));
			yyjson_mut_obj_add_strcpy(doc, metadata_value_obj, key_ptr, value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "metadata", metadata_value_obj);
	}
}

yyjson_mut_val *ScanReport::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
