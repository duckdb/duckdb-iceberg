
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

TableMetadata TableMetadata::Copy() const {
	TableMetadata res;
	res.format_version = format_version;
	res.table_uuid = table_uuid;
	if (location.has_value()) {
		res.location.emplace();
		(*res.location) = (*location);
	}
	if (last_updated_ms.has_value()) {
		res.last_updated_ms.emplace();
		(*res.last_updated_ms) = (*last_updated_ms);
	}
	if (next_row_id.has_value()) {
		res.next_row_id.emplace();
		(*res.next_row_id) = (*next_row_id);
	}
	if (properties.has_value()) {
		res.properties.emplace();
		for (auto &entry : (*properties)) {
			(*res.properties).emplace(entry.first, entry.second);
		}
	}
	if (schemas.has_value()) {
		res.schemas.emplace();
		(*res.schemas).reserve((*schemas).size());
		for (auto &item : (*schemas)) {
			(*res.schemas).emplace_back(item.Copy());
		}
	}
	if (current_schema_id.has_value()) {
		res.current_schema_id.emplace();
		(*res.current_schema_id) = (*current_schema_id);
	}
	if (last_column_id.has_value()) {
		res.last_column_id.emplace();
		(*res.last_column_id) = (*last_column_id);
	}
	if (partition_specs.has_value()) {
		res.partition_specs.emplace();
		(*res.partition_specs).reserve((*partition_specs).size());
		for (auto &item : (*partition_specs)) {
			(*res.partition_specs).emplace_back(item.Copy());
		}
	}
	if (default_spec_id.has_value()) {
		res.default_spec_id.emplace();
		(*res.default_spec_id) = (*default_spec_id);
	}
	if (last_partition_id.has_value()) {
		res.last_partition_id.emplace();
		(*res.last_partition_id) = (*last_partition_id);
	}
	if (sort_orders.has_value()) {
		res.sort_orders.emplace();
		(*res.sort_orders).reserve((*sort_orders).size());
		for (auto &item : (*sort_orders)) {
			(*res.sort_orders).emplace_back(item.Copy());
		}
	}
	if (default_sort_order_id.has_value()) {
		res.default_sort_order_id.emplace();
		(*res.default_sort_order_id) = (*default_sort_order_id);
	}
	if (encryption_keys.has_value()) {
		res.encryption_keys.emplace();
		(*res.encryption_keys).reserve((*encryption_keys).size());
		for (auto &item : (*encryption_keys)) {
			(*res.encryption_keys).emplace_back(item.Copy());
		}
	}
	if (snapshots.has_value()) {
		res.snapshots.emplace();
		(*res.snapshots).reserve((*snapshots).size());
		for (auto &item : (*snapshots)) {
			(*res.snapshots).emplace_back(item.Copy());
		}
	}
	if (refs.has_value()) {
		res.refs.emplace();
		(*res.refs) = (*refs).Copy();
	}
	if (current_snapshot_id.has_value()) {
		res.current_snapshot_id.emplace();
		(*res.current_snapshot_id) = (*current_snapshot_id);
	}
	if (last_sequence_number.has_value()) {
		res.last_sequence_number.emplace();
		(*res.last_sequence_number) = (*last_sequence_number);
	}
	if (snapshot_log.has_value()) {
		res.snapshot_log.emplace();
		(*res.snapshot_log) = (*snapshot_log).Copy();
	}
	if (metadata_log.has_value()) {
		res.metadata_log.emplace();
		(*res.metadata_log) = (*metadata_log).Copy();
	}
	if (statistics.has_value()) {
		res.statistics.emplace();
		(*res.statistics).reserve((*statistics).size());
		for (auto &item : (*statistics)) {
			(*res.statistics).emplace_back(item.Copy());
		}
	}
	if (partition_statistics.has_value()) {
		res.partition_statistics.emplace();
		(*res.partition_statistics).reserve((*partition_statistics).size());
		for (auto &item : (*partition_statistics)) {
			(*res.partition_statistics).emplace_back(item.Copy());
		}
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
		string location_tmp;
		if (yyjson_is_str(location_val)) {
			location_tmp = yyjson_get_str(location_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'location_tmp' is not of type 'string', found '%s' instead",
			    yyjson_get_type_desc(location_val));
		}
		location = std::move(location_tmp);
	}
	auto last_updated_ms_val = yyjson_obj_get(obj, "last-updated-ms");
	if (last_updated_ms_val) {
		int64_t last_updated_ms_tmp;
		if (yyjson_is_sint(last_updated_ms_val)) {
			last_updated_ms_tmp = yyjson_get_sint(last_updated_ms_val);
		} else if (yyjson_is_uint(last_updated_ms_val)) {
			last_updated_ms_tmp = yyjson_get_uint(last_updated_ms_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'last_updated_ms_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(last_updated_ms_val));
		}
		last_updated_ms = std::move(last_updated_ms_tmp);
	}
	auto next_row_id_val = yyjson_obj_get(obj, "next-row-id");
	if (next_row_id_val) {
		int64_t next_row_id_tmp;
		if (yyjson_is_sint(next_row_id_val)) {
			next_row_id_tmp = yyjson_get_sint(next_row_id_val);
		} else if (yyjson_is_uint(next_row_id_val)) {
			next_row_id_tmp = yyjson_get_uint(next_row_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'next_row_id_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(next_row_id_val));
		}
		next_row_id = std::move(next_row_id_tmp);
	}
	auto properties_val = yyjson_obj_get(obj, "properties");
	if (properties_val) {
		case_insensitive_map_t<string> properties_tmp;
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
				properties_tmp.emplace(key_str, std::move(tmp));
			}
		} else {
			return "TableMetadata property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	auto schemas_val = yyjson_obj_get(obj, "schemas");
	if (schemas_val) {
		vector<Schema> schemas_tmp;
		if (yyjson_is_arr(schemas_val)) {
			size_t schemas_tmp_idx, schemas_tmp_max;
			yyjson_val *schemas_tmp_item_val;
			yyjson_arr_foreach(schemas_val, schemas_tmp_idx, schemas_tmp_max, schemas_tmp_item_val) {
				Schema schemas_tmp_item;
				error = schemas_tmp_item.TryFromJSON(schemas_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				schemas_tmp.emplace_back(std::move(schemas_tmp_item));
			}
		} else {
			return StringUtil::Format("TableMetadata property 'schemas_tmp' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(schemas_val));
		}
		schemas = std::move(schemas_tmp);
	}
	auto current_schema_id_val = yyjson_obj_get(obj, "current-schema-id");
	if (current_schema_id_val) {
		int32_t current_schema_id_tmp;
		if (yyjson_is_int(current_schema_id_val)) {
			current_schema_id_tmp = yyjson_get_int(current_schema_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'current_schema_id_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(current_schema_id_val));
		}
		current_schema_id = std::move(current_schema_id_tmp);
	}
	auto last_column_id_val = yyjson_obj_get(obj, "last-column-id");
	if (last_column_id_val) {
		int32_t last_column_id_tmp;
		if (yyjson_is_int(last_column_id_val)) {
			last_column_id_tmp = yyjson_get_int(last_column_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'last_column_id_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(last_column_id_val));
		}
		last_column_id = std::move(last_column_id_tmp);
	}
	auto partition_specs_val = yyjson_obj_get(obj, "partition-specs");
	if (partition_specs_val) {
		vector<PartitionSpec> partition_specs_tmp;
		if (yyjson_is_arr(partition_specs_val)) {
			size_t partition_specs_tmp_idx, partition_specs_tmp_max;
			yyjson_val *partition_specs_tmp_item_val;
			yyjson_arr_foreach(partition_specs_val, partition_specs_tmp_idx, partition_specs_tmp_max,
			                   partition_specs_tmp_item_val) {
				PartitionSpec partition_specs_tmp_item;
				error = partition_specs_tmp_item.TryFromJSON(partition_specs_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				partition_specs_tmp.emplace_back(std::move(partition_specs_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'partition_specs_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(partition_specs_val));
		}
		partition_specs = std::move(partition_specs_tmp);
	}
	auto default_spec_id_val = yyjson_obj_get(obj, "default-spec-id");
	if (default_spec_id_val) {
		int32_t default_spec_id_tmp;
		if (yyjson_is_int(default_spec_id_val)) {
			default_spec_id_tmp = yyjson_get_int(default_spec_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'default_spec_id_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(default_spec_id_val));
		}
		default_spec_id = std::move(default_spec_id_tmp);
	}
	auto last_partition_id_val = yyjson_obj_get(obj, "last-partition-id");
	if (last_partition_id_val) {
		int32_t last_partition_id_tmp;
		if (yyjson_is_int(last_partition_id_val)) {
			last_partition_id_tmp = yyjson_get_int(last_partition_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'last_partition_id_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(last_partition_id_val));
		}
		last_partition_id = std::move(last_partition_id_tmp);
	}
	auto sort_orders_val = yyjson_obj_get(obj, "sort-orders");
	if (sort_orders_val) {
		vector<SortOrder> sort_orders_tmp;
		if (yyjson_is_arr(sort_orders_val)) {
			size_t sort_orders_tmp_idx, sort_orders_tmp_max;
			yyjson_val *sort_orders_tmp_item_val;
			yyjson_arr_foreach(sort_orders_val, sort_orders_tmp_idx, sort_orders_tmp_max, sort_orders_tmp_item_val) {
				SortOrder sort_orders_tmp_item;
				error = sort_orders_tmp_item.TryFromJSON(sort_orders_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				sort_orders_tmp.emplace_back(std::move(sort_orders_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'sort_orders_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(sort_orders_val));
		}
		sort_orders = std::move(sort_orders_tmp);
	}
	auto default_sort_order_id_val = yyjson_obj_get(obj, "default-sort-order-id");
	if (default_sort_order_id_val) {
		int32_t default_sort_order_id_tmp;
		if (yyjson_is_int(default_sort_order_id_val)) {
			default_sort_order_id_tmp = yyjson_get_int(default_sort_order_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'default_sort_order_id_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(default_sort_order_id_val));
		}
		default_sort_order_id = std::move(default_sort_order_id_tmp);
	}
	auto encryption_keys_val = yyjson_obj_get(obj, "encryption-keys");
	if (encryption_keys_val) {
		vector<EncryptedKey> encryption_keys_tmp;
		if (yyjson_is_arr(encryption_keys_val)) {
			size_t encryption_keys_tmp_idx, encryption_keys_tmp_max;
			yyjson_val *encryption_keys_tmp_item_val;
			yyjson_arr_foreach(encryption_keys_val, encryption_keys_tmp_idx, encryption_keys_tmp_max,
			                   encryption_keys_tmp_item_val) {
				EncryptedKey encryption_keys_tmp_item;
				error = encryption_keys_tmp_item.TryFromJSON(encryption_keys_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				encryption_keys_tmp.emplace_back(std::move(encryption_keys_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'encryption_keys_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(encryption_keys_val));
		}
		encryption_keys = std::move(encryption_keys_tmp);
	}
	auto snapshots_val = yyjson_obj_get(obj, "snapshots");
	if (snapshots_val) {
		vector<Snapshot> snapshots_tmp;
		if (yyjson_is_arr(snapshots_val)) {
			size_t snapshots_tmp_idx, snapshots_tmp_max;
			yyjson_val *snapshots_tmp_item_val;
			yyjson_arr_foreach(snapshots_val, snapshots_tmp_idx, snapshots_tmp_max, snapshots_tmp_item_val) {
				Snapshot snapshots_tmp_item;
				error = snapshots_tmp_item.TryFromJSON(snapshots_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				snapshots_tmp.emplace_back(std::move(snapshots_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'snapshots_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(snapshots_val));
		}
		snapshots = std::move(snapshots_tmp);
	}
	auto refs_val = yyjson_obj_get(obj, "refs");
	if (refs_val) {
		SnapshotReferences refs_tmp;
		error = refs_tmp.TryFromJSON(refs_val);
		if (!error.empty()) {
			return error;
		}
		refs = std::move(refs_tmp);
	}
	auto current_snapshot_id_val = yyjson_obj_get(obj, "current-snapshot-id");
	if (current_snapshot_id_val) {
		if (yyjson_is_null(current_snapshot_id_val)) {
			//! do nothing, property is explicitly nullable
		} else {
			int64_t current_snapshot_id_tmp;
			if (yyjson_is_sint(current_snapshot_id_val)) {
				current_snapshot_id_tmp = yyjson_get_sint(current_snapshot_id_val);
			} else if (yyjson_is_uint(current_snapshot_id_val)) {
				current_snapshot_id_tmp = yyjson_get_uint(current_snapshot_id_val);
			} else {
				return StringUtil::Format(
				    "TableMetadata property 'current_snapshot_id_tmp' is not of type 'integer', found '%s' instead",
				    yyjson_get_type_desc(current_snapshot_id_val));
			}
			current_snapshot_id = std::move(current_snapshot_id_tmp);
		}
	}
	auto last_sequence_number_val = yyjson_obj_get(obj, "last-sequence-number");
	if (last_sequence_number_val) {
		int64_t last_sequence_number_tmp;
		if (yyjson_is_sint(last_sequence_number_val)) {
			last_sequence_number_tmp = yyjson_get_sint(last_sequence_number_val);
		} else if (yyjson_is_uint(last_sequence_number_val)) {
			last_sequence_number_tmp = yyjson_get_uint(last_sequence_number_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'last_sequence_number_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(last_sequence_number_val));
		}
		last_sequence_number = std::move(last_sequence_number_tmp);
	}
	auto snapshot_log_val = yyjson_obj_get(obj, "snapshot-log");
	if (snapshot_log_val) {
		SnapshotLog snapshot_log_tmp;
		error = snapshot_log_tmp.TryFromJSON(snapshot_log_val);
		if (!error.empty()) {
			return error;
		}
		snapshot_log = std::move(snapshot_log_tmp);
	}
	auto metadata_log_val = yyjson_obj_get(obj, "metadata-log");
	if (metadata_log_val) {
		MetadataLog metadata_log_tmp;
		error = metadata_log_tmp.TryFromJSON(metadata_log_val);
		if (!error.empty()) {
			return error;
		}
		metadata_log = std::move(metadata_log_tmp);
	}
	auto statistics_val = yyjson_obj_get(obj, "statistics");
	if (statistics_val) {
		vector<StatisticsFile> statistics_tmp;
		if (yyjson_is_arr(statistics_val)) {
			size_t statistics_tmp_idx, statistics_tmp_max;
			yyjson_val *statistics_tmp_item_val;
			yyjson_arr_foreach(statistics_val, statistics_tmp_idx, statistics_tmp_max, statistics_tmp_item_val) {
				StatisticsFile statistics_tmp_item;
				error = statistics_tmp_item.TryFromJSON(statistics_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				statistics_tmp.emplace_back(std::move(statistics_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'statistics_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(statistics_val));
		}
		statistics = std::move(statistics_tmp);
	}
	auto partition_statistics_val = yyjson_obj_get(obj, "partition-statistics");
	if (partition_statistics_val) {
		vector<PartitionStatisticsFile> partition_statistics_tmp;
		if (yyjson_is_arr(partition_statistics_val)) {
			size_t partition_statistics_tmp_idx, partition_statistics_tmp_max;
			yyjson_val *partition_statistics_tmp_item_val;
			yyjson_arr_foreach(partition_statistics_val, partition_statistics_tmp_idx, partition_statistics_tmp_max,
			                   partition_statistics_tmp_item_val) {
				PartitionStatisticsFile partition_statistics_tmp_item;
				error = partition_statistics_tmp_item.TryFromJSON(partition_statistics_tmp_item_val);
				if (!error.empty()) {
					return error;
				}
				partition_statistics_tmp.emplace_back(std::move(partition_statistics_tmp_item));
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'partition_statistics_tmp' is not of type 'array', found '%s' instead",
			    yyjson_get_type_desc(partition_statistics_val));
		}
		partition_statistics = std::move(partition_statistics_tmp);
	}
	return "";
}

void TableMetadata::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: format-version
	yyjson_mut_obj_add_int(doc, obj, "format-version", format_version);

	// Serialize: table-uuid
	yyjson_mut_obj_add_strcpy(doc, obj, "table-uuid", table_uuid.c_str());

	// Serialize: location
	if (location.has_value()) {
		auto &location_value = *location;
		yyjson_mut_obj_add_strcpy(doc, obj, "location", location_value.c_str());
	}

	// Serialize: last-updated-ms
	if (last_updated_ms.has_value()) {
		auto &last_updated_ms_value = *last_updated_ms;
		yyjson_mut_obj_add_sint(doc, obj, "last-updated-ms", last_updated_ms_value);
	}

	// Serialize: next-row-id
	if (next_row_id.has_value()) {
		auto &next_row_id_value = *next_row_id;
		yyjson_mut_obj_add_sint(doc, obj, "next-row-id", next_row_id_value);
	}

	// Serialize: properties
	if (properties.has_value()) {
		auto &properties_value = *properties;
		yyjson_mut_val *properties_value_obj = yyjson_mut_obj(doc);
		for (const auto &it : properties_value) {
			auto &key = it.first;
			auto &value = it.second;
			auto key_ptr = unsafe_yyjson_mut_strncpy(doc, key.c_str(), strlen(key.c_str()));
			yyjson_mut_obj_add_strcpy(doc, properties_value_obj, key_ptr, value.c_str());
		}
		yyjson_mut_obj_add_val(doc, obj, "properties", properties_value_obj);
	}

	// Serialize: schemas
	if (schemas.has_value()) {
		auto &schemas_value = *schemas;
		yyjson_mut_val *schemas_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : schemas_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(schemas_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "schemas", schemas_value_arr);
	}

	// Serialize: current-schema-id
	if (current_schema_id.has_value()) {
		auto &current_schema_id_value = *current_schema_id;
		yyjson_mut_obj_add_int(doc, obj, "current-schema-id", current_schema_id_value);
	}

	// Serialize: last-column-id
	if (last_column_id.has_value()) {
		auto &last_column_id_value = *last_column_id;
		yyjson_mut_obj_add_int(doc, obj, "last-column-id", last_column_id_value);
	}

	// Serialize: partition-specs
	if (partition_specs.has_value()) {
		auto &partition_specs_value = *partition_specs;
		yyjson_mut_val *partition_specs_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : partition_specs_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(partition_specs_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "partition-specs", partition_specs_value_arr);
	}

	// Serialize: default-spec-id
	if (default_spec_id.has_value()) {
		auto &default_spec_id_value = *default_spec_id;
		yyjson_mut_obj_add_int(doc, obj, "default-spec-id", default_spec_id_value);
	}

	// Serialize: last-partition-id
	if (last_partition_id.has_value()) {
		auto &last_partition_id_value = *last_partition_id;
		yyjson_mut_obj_add_int(doc, obj, "last-partition-id", last_partition_id_value);
	}

	// Serialize: sort-orders
	if (sort_orders.has_value()) {
		auto &sort_orders_value = *sort_orders;
		yyjson_mut_val *sort_orders_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : sort_orders_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(sort_orders_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "sort-orders", sort_orders_value_arr);
	}

	// Serialize: default-sort-order-id
	if (default_sort_order_id.has_value()) {
		auto &default_sort_order_id_value = *default_sort_order_id;
		yyjson_mut_obj_add_int(doc, obj, "default-sort-order-id", default_sort_order_id_value);
	}

	// Serialize: encryption-keys
	if (encryption_keys.has_value()) {
		auto &encryption_keys_value = *encryption_keys;
		yyjson_mut_val *encryption_keys_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : encryption_keys_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(encryption_keys_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "encryption-keys", encryption_keys_value_arr);
	}

	// Serialize: snapshots
	if (snapshots.has_value()) {
		auto &snapshots_value = *snapshots;
		yyjson_mut_val *snapshots_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : snapshots_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(snapshots_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "snapshots", snapshots_value_arr);
	}

	// Serialize: refs
	if (refs.has_value()) {
		auto &refs_value = *refs;
		yyjson_mut_val *refs_value_val = refs_value.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "refs", refs_value_val);
	}

	// Serialize: current-snapshot-id
	if (current_snapshot_id.has_value()) {
		auto &current_snapshot_id_value = *current_snapshot_id;
		yyjson_mut_obj_add_sint(doc, obj, "current-snapshot-id", current_snapshot_id_value);
	}

	// Serialize: last-sequence-number
	if (last_sequence_number.has_value()) {
		auto &last_sequence_number_value = *last_sequence_number;
		yyjson_mut_obj_add_sint(doc, obj, "last-sequence-number", last_sequence_number_value);
	}

	// Serialize: snapshot-log
	if (snapshot_log.has_value()) {
		auto &snapshot_log_value = *snapshot_log;
		yyjson_mut_val *snapshot_log_value_val = snapshot_log_value.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "snapshot-log", snapshot_log_value_val);
	}

	// Serialize: metadata-log
	if (metadata_log.has_value()) {
		auto &metadata_log_value = *metadata_log;
		yyjson_mut_val *metadata_log_value_val = metadata_log_value.ToJSON(doc);
		yyjson_mut_obj_add_val(doc, obj, "metadata-log", metadata_log_value_val);
	}

	// Serialize: statistics
	if (statistics.has_value()) {
		auto &statistics_value = *statistics;
		yyjson_mut_val *statistics_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : statistics_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(statistics_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "statistics", statistics_value_arr);
	}

	// Serialize: partition-statistics
	if (partition_statistics.has_value()) {
		auto &partition_statistics_value = *partition_statistics;
		yyjson_mut_val *partition_statistics_value_arr = yyjson_mut_arr(doc);
		for (const auto &item : partition_statistics_value) {
			yyjson_mut_val *item_val = item.ToJSON(doc);
			yyjson_mut_arr_append(partition_statistics_value_arr, item_val);
		}
		yyjson_mut_obj_add_val(doc, obj, "partition-statistics", partition_statistics_value_arr);
	}
}

yyjson_mut_val *TableMetadata::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
