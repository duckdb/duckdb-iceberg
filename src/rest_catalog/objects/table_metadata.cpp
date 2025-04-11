
#include "rest_catalog/objects/table_metadata.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
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
		format_version = yyjson_get_sint(format_version_val);
	}
	auto table_uuid_val = yyjson_obj_get(obj, "table-uuid");
	if (!table_uuid_val) {
		return "TableMetadata required property 'table-uuid' is missing";
	} else {
		table_uuid = yyjson_get_str(table_uuid_val);
	}
	auto location_val = yyjson_obj_get(obj, "location");
	if (location_val) {
		has_location = true;
		location = yyjson_get_str(location_val);
	}
	auto last_updated_ms_val = yyjson_obj_get(obj, "last-updated-ms");
	if (last_updated_ms_val) {
		has_last_updated_ms = true;
		last_updated_ms = yyjson_get_sint(last_updated_ms_val);
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		has_properties = true;
		properties = parse_object_of_strings(properties_val);
	}
	auto schemas_val = yyjson_obj_get(obj, "schemas");
	if (schemas_val) {
		has_schemas = true;
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
	}
	auto current_schema_id_val = yyjson_obj_get(obj, "current-schema-id");
	if (current_schema_id_val) {
		has_current_schema_id = true;
		current_schema_id = yyjson_get_sint(current_schema_id_val);
	}
	auto last_column_id_val = yyjson_obj_get(obj, "last-column-id");
	if (last_column_id_val) {
		has_last_column_id = true;
		last_column_id = yyjson_get_sint(last_column_id_val);
	}
	auto partition_specs_val = yyjson_obj_get(obj, "partition-specs");
	if (partition_specs_val) {
		has_partition_specs = true;
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
	}
	auto default_spec_id_val = yyjson_obj_get(obj, "default-spec-id");
	if (default_spec_id_val) {
		has_default_spec_id = true;
		default_spec_id = yyjson_get_sint(default_spec_id_val);
	}
	auto last_partition_id_val = yyjson_obj_get(obj, "last-partition-id");
	if (last_partition_id_val) {
		has_last_partition_id = true;
		last_partition_id = yyjson_get_sint(last_partition_id_val);
	}
	auto sort_orders_val = yyjson_obj_get(obj, "sort-orders");
	if (sort_orders_val) {
		has_sort_orders = true;
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
	}
	auto default_sort_order_id_val = yyjson_obj_get(obj, "default-sort-order-id");
	if (default_sort_order_id_val) {
		has_default_sort_order_id = true;
		default_sort_order_id = yyjson_get_sint(default_sort_order_id_val);
	}
	auto snapshots_val = yyjson_obj_get(obj, "snapshots");
	if (snapshots_val) {
		has_snapshots = true;
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
		current_snapshot_id = yyjson_get_sint(current_snapshot_id_val);
	}
	auto last_sequence_number_val = yyjson_obj_get(obj, "last-sequence-number");
	if (last_sequence_number_val) {
		has_last_sequence_number = true;
		last_sequence_number = yyjson_get_sint(last_sequence_number_val);
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
	}
	auto partition_statistics_val = yyjson_obj_get(obj, "partition-statistics");
	if (partition_statistics_val) {
		has_partition_statistics = true;
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
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
