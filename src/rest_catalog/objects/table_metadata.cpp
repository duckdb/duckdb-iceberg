
#include "rest_catalog/objects/table_metadata.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

TableMetadata::TableMetadata() {
}

TableMetadata TableMetadata::FromJSON(yyjson_val *obj) {
	TableMetadata res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string TableMetadata::TryFromJSON(yyjson_val *obj) {
	string error;
	auto format_version_val = yyjson_obj_get(obj, "format-version");
	if (!format_version_val) {
		return "TableMetadata required property 'format-version' is missing";
	} else {
		if (yyjson_is_int(format_version_val)) {
			format_version = yyjson_get_int(format_version_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'format_version' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(format_version_val));
		}
	}
	auto table_uuid_val = yyjson_obj_get(obj, "table-uuid");
	if (!table_uuid_val) {
		return "TableMetadata required property 'table-uuid' is missing";
	} else {
		if (yyjson_is_str(table_uuid_val)) {
			table_uuid = yyjson_get_str(table_uuid_val);
		} else {
			return StringUtil::Format("TableMetadata property 'table_uuid' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(table_uuid_val));
		}
	}
	auto location_val = yyjson_obj_get(obj, "location");
	if (location_val) {
		has_location = true;
		if (yyjson_is_str(location_val)) {
			location = yyjson_get_str(location_val);
		} else {
			return StringUtil::Format("TableMetadata property 'location' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(location_val));
		}
	}
	auto last_updated_ms_val = yyjson_obj_get(obj, "last-updated-ms");
	if (last_updated_ms_val) {
		has_last_updated_ms = true;
		if (yyjson_is_sint(last_updated_ms_val)) {
			last_updated_ms = yyjson_get_sint(last_updated_ms_val);
		} else if (yyjson_is_uint(last_updated_ms_val)) {
			last_updated_ms = yyjson_get_uint(last_updated_ms_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'last_updated_ms' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(last_updated_ms_val));
		}
	}
	auto next_row_id_val = yyjson_obj_get(obj, "next-row-id");
	if (next_row_id_val) {
		has_next_row_id = true;
		if (yyjson_is_sint(next_row_id_val)) {
			next_row_id = yyjson_get_sint(next_row_id_val);
		} else if (yyjson_is_uint(next_row_id_val)) {
			next_row_id = yyjson_get_uint(next_row_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'next_row_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(next_row_id_val));
		}
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		has_properties = true;
		if (yyjson_is_obj(properties_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(properties_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format(
					    "TableMetadata property 'tmp' is not of type 'string', found '%s' instead",
					    yyjson_get_type_desc(val));
				}
				properties.emplace(key_str, std::move(tmp));
			}
		} else {
			return "TableMetadata property 'properties' is not of type 'object'";
		}
	}
	auto schemas_val = yyjson_obj_get(obj, "schemas");
	if (schemas_val) {
		has_schemas = true;
		if (yyjson_is_arr(schemas_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(schemas_val, idx, max, val) {
				Schema tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				schemas.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("TableMetadata property 'schemas' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(schemas_val));
		}
	}
	auto current_schema_id_val = yyjson_obj_get(obj, "current-schema-id");
	if (current_schema_id_val) {
		has_current_schema_id = true;
		if (yyjson_is_int(current_schema_id_val)) {
			current_schema_id = yyjson_get_int(current_schema_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'current_schema_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(current_schema_id_val));
		}
	}
	auto last_column_id_val = yyjson_obj_get(obj, "last-column-id");
	if (last_column_id_val) {
		has_last_column_id = true;
		if (yyjson_is_int(last_column_id_val)) {
			last_column_id = yyjson_get_int(last_column_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'last_column_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(last_column_id_val));
		}
	}
	auto partition_specs_val = yyjson_obj_get(obj, "partition-specs");
	if (partition_specs_val) {
		has_partition_specs = true;
		if (yyjson_is_arr(partition_specs_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(partition_specs_val, idx, max, val) {
				PartitionSpec tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				partition_specs.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'partition_specs' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(partition_specs_val));
		}
	}
	auto default_spec_id_val = yyjson_obj_get(obj, "default-spec-id");
	if (default_spec_id_val) {
		has_default_spec_id = true;
		if (yyjson_is_int(default_spec_id_val)) {
			default_spec_id = yyjson_get_int(default_spec_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'default_spec_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(default_spec_id_val));
		}
	}
	auto last_partition_id_val = yyjson_obj_get(obj, "last-partition-id");
	if (last_partition_id_val) {
		has_last_partition_id = true;
		if (yyjson_is_int(last_partition_id_val)) {
			last_partition_id = yyjson_get_int(last_partition_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'last_partition_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(last_partition_id_val));
		}
	}
	auto sort_orders_val = yyjson_obj_get(obj, "sort-orders");
	if (sort_orders_val) {
		has_sort_orders = true;
		if (yyjson_is_arr(sort_orders_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(sort_orders_val, idx, max, val) {
				SortOrder tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				sort_orders.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("TableMetadata property 'sort_orders' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(sort_orders_val));
		}
	}
	auto default_sort_order_id_val = yyjson_obj_get(obj, "default-sort-order-id");
	if (default_sort_order_id_val) {
		has_default_sort_order_id = true;
		if (yyjson_is_int(default_sort_order_id_val)) {
			default_sort_order_id = yyjson_get_int(default_sort_order_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'default_sort_order_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(default_sort_order_id_val));
		}
	}
	auto encryption_keys_val = yyjson_obj_get(obj, "encryption-keys");
	if (encryption_keys_val) {
		has_encryption_keys = true;
		if (yyjson_is_arr(encryption_keys_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(encryption_keys_val, idx, max, val) {
				EncryptedKey tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				encryption_keys.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'encryption_keys' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(encryption_keys_val));
		}
	}
	auto snapshots_val = yyjson_obj_get(obj, "snapshots");
	if (snapshots_val) {
		has_snapshots = true;
		if (yyjson_is_arr(snapshots_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(snapshots_val, idx, max, val) {
				Snapshot tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				snapshots.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("TableMetadata property 'snapshots' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(snapshots_val));
		}
	}
	auto refs_val = yyjson_obj_get(obj, "refs");
	if (refs_val) {
		has_refs = true;
		error = refs.TryFromJSON(refs_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto current_snapshot_id_val = yyjson_obj_get(obj, "current-snapshot-id");
	if (current_snapshot_id_val) {
		has_current_snapshot_id = true;
		if (yyjson_is_null(current_snapshot_id_val)) {
			//! do nothing, property is explicitly nullable
			has_current_snapshot_id = false;
		} else if (yyjson_is_sint(current_snapshot_id_val)) {
			current_snapshot_id = yyjson_get_sint(current_snapshot_id_val);
		} else if (yyjson_is_uint(current_snapshot_id_val)) {
			current_snapshot_id = yyjson_get_uint(current_snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'current_snapshot_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(current_snapshot_id_val));
		}
	}
	auto last_sequence_number_val = yyjson_obj_get(obj, "last-sequence-number");
	if (last_sequence_number_val) {
		has_last_sequence_number = true;
		if (yyjson_is_sint(last_sequence_number_val)) {
			last_sequence_number = yyjson_get_sint(last_sequence_number_val);
		} else if (yyjson_is_uint(last_sequence_number_val)) {
			last_sequence_number = yyjson_get_uint(last_sequence_number_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'last_sequence_number' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(last_sequence_number_val));
		}
	}
	auto snapshot_log_val = yyjson_obj_get(obj, "snapshot-log");
	if (snapshot_log_val) {
		has_snapshot_log = true;
		error = snapshot_log.TryFromJSON(snapshot_log_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto metadata_log_val = yyjson_obj_get(obj, "metadata-log");
	if (metadata_log_val) {
		has_metadata_log = true;
		error = metadata_log.TryFromJSON(metadata_log_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto statistics_val = yyjson_obj_get(obj, "statistics");
	if (statistics_val) {
		has_statistics = true;
		if (yyjson_is_arr(statistics_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(statistics_val, idx, max, val) {
				StatisticsFile tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				statistics.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format("TableMetadata property 'statistics' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(statistics_val));
		}
	}
	auto partition_statistics_val = yyjson_obj_get(obj, "partition-statistics");
	if (partition_statistics_val) {
		has_partition_statistics = true;
		if (yyjson_is_arr(partition_statistics_val)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(partition_statistics_val, idx, max, val) {
				PartitionStatisticsFile tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				partition_statistics.emplace_back(std::move(tmp));
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'partition_statistics' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(partition_statistics_val));
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
