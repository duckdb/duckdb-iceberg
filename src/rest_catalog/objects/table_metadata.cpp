
#include "rest_catalog/objects/table_metadata.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

TableMetadata::TableMetadata() {
}

TableMetadata TableMetadata::FromJSON(JSONValue obj) {
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
	if (schema.has_value()) {
		res.schema.emplace();
		(*res.schema) = (*schema).Copy();
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
	if (partition_spec.has_value()) {
		res.partition_spec.emplace();
		(*res.partition_spec).reserve((*partition_spec).size());
		for (auto &item : (*partition_spec)) {
			(*res.partition_spec).emplace_back(item.Copy());
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

string TableMetadata::TryFromJSON(JSONValue obj) {
	string error;
	auto format_version_val = obj.GetMember("format-version");
	if (!format_version_val.IsValid()) {
		return "TableMetadata required property 'format-version' is missing";
	} else {
		if (json_utils::IsInteger(format_version_val)) {
			format_version = json_utils::GetSignedInteger(format_version_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'format_version' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(format_version_val).c_str());
		}
	}
	auto table_uuid_val = obj.GetMember("table-uuid");
	if (!table_uuid_val.IsValid()) {
		return "TableMetadata required property 'table-uuid' is missing";
	} else {
		if (json_utils::IsString(table_uuid_val)) {
			table_uuid = json_utils::GetString(table_uuid_val);
		} else {
			return StringUtil::Format("TableMetadata property 'table_uuid' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(table_uuid_val).c_str());
		}
	}
	auto location_val = obj.GetMember("location");
	if (location_val.IsValid()) {
		string location_tmp;
		if (json_utils::IsString(location_val)) {
			location_tmp = json_utils::GetString(location_val);
		} else {
			return StringUtil::Format("TableMetadata property 'location_tmp' is not of type 'string', found %s instead",
			                          json_utils::GetTypeDescription(location_val).c_str());
		}
		location = std::move(location_tmp);
	}
	auto last_updated_ms_val = obj.GetMember("last-updated-ms");
	if (last_updated_ms_val.IsValid()) {
		int64_t last_updated_ms_tmp;
		if (json_utils::IsInteger(last_updated_ms_val)) {
			last_updated_ms_tmp = json_utils::GetSignedInteger(last_updated_ms_val);
		} else if (json_utils::IsUnsignedInteger(last_updated_ms_val)) {
			last_updated_ms_tmp = json_utils::GetUnsignedInteger(last_updated_ms_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'last_updated_ms_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(last_updated_ms_val).c_str());
		}
		last_updated_ms = std::move(last_updated_ms_tmp);
	}
	auto next_row_id_val = obj.GetMember("next-row-id");
	if (next_row_id_val.IsValid()) {
		int64_t next_row_id_tmp;
		if (json_utils::IsInteger(next_row_id_val)) {
			next_row_id_tmp = json_utils::GetSignedInteger(next_row_id_val);
		} else if (json_utils::IsUnsignedInteger(next_row_id_val)) {
			next_row_id_tmp = json_utils::GetUnsignedInteger(next_row_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'next_row_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(next_row_id_val).c_str());
		}
		next_row_id = std::move(next_row_id_tmp);
	}
	auto properties_val = obj.GetMember("properties");
	if (properties_val.IsValid()) {
		case_insensitive_map_t<string> properties_tmp;
		if (properties_val.IsObject()) {
			properties_val.IterateObject([&](const string &key_str, JSONValue val) {
				if (!error.empty()) {
					return;
				}
				string tmp;
				if (json_utils::IsString(val)) {
					tmp = json_utils::GetString(val);
				} else {
					error = StringUtil::Format("TableMetadata property 'tmp' is not of type 'string', found %s instead",
					                           json_utils::GetTypeDescription(val).c_str());
					return;
				}
				properties_tmp.emplace(key_str, std::move(tmp));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return "TableMetadata property 'properties_tmp' is not of type 'object'";
		}
		properties = std::move(properties_tmp);
	}
	auto schemas_val = obj.GetMember("schemas");
	if (schemas_val.IsValid()) {
		vector<Schema> schemas_tmp;
		if (schemas_val.IsArray()) {
			schemas_val.IterateArray([&](JSONValue schemas_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				Schema schemas_tmp_item;
				error = schemas_tmp_item.TryFromJSON(schemas_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				schemas_tmp.emplace_back(std::move(schemas_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("TableMetadata property 'schemas_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(schemas_val).c_str());
		}
		schemas = std::move(schemas_tmp);
	}
	auto schema_val = obj.GetMember("schema");
	if (schema_val.IsValid()) {
		Schema schema_tmp;
		error = schema_tmp.TryFromJSON(schema_val);
		if (!error.empty()) {
			return error;
		}
		schema = std::move(schema_tmp);
	}
	auto current_schema_id_val = obj.GetMember("current-schema-id");
	if (current_schema_id_val.IsValid()) {
		int32_t current_schema_id_tmp;
		if (json_utils::IsInteger(current_schema_id_val)) {
			current_schema_id_tmp = json_utils::GetSignedInteger(current_schema_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'current_schema_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(current_schema_id_val).c_str());
		}
		current_schema_id = std::move(current_schema_id_tmp);
	}
	auto last_column_id_val = obj.GetMember("last-column-id");
	if (last_column_id_val.IsValid()) {
		int32_t last_column_id_tmp;
		if (json_utils::IsInteger(last_column_id_val)) {
			last_column_id_tmp = json_utils::GetSignedInteger(last_column_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'last_column_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(last_column_id_val).c_str());
		}
		last_column_id = std::move(last_column_id_tmp);
	}
	auto partition_specs_val = obj.GetMember("partition-specs");
	if (partition_specs_val.IsValid()) {
		vector<PartitionSpec> partition_specs_tmp;
		if (partition_specs_val.IsArray()) {
			partition_specs_val.IterateArray([&](JSONValue partition_specs_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				PartitionSpec partition_specs_tmp_item;
				error = partition_specs_tmp_item.TryFromJSON(partition_specs_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				partition_specs_tmp.emplace_back(std::move(partition_specs_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'partition_specs_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(partition_specs_val).c_str());
		}
		partition_specs = std::move(partition_specs_tmp);
	}
	auto partition_spec_val = obj.GetMember("partition-spec");
	if (partition_spec_val.IsValid()) {
		vector<PartitionField> partition_spec_tmp;
		if (partition_spec_val.IsArray()) {
			partition_spec_val.IterateArray([&](JSONValue partition_spec_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				PartitionField partition_spec_tmp_item;
				error = partition_spec_tmp_item.TryFromJSON(partition_spec_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				partition_spec_tmp.emplace_back(std::move(partition_spec_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'partition_spec_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(partition_spec_val).c_str());
		}
		partition_spec = std::move(partition_spec_tmp);
	}
	auto default_spec_id_val = obj.GetMember("default-spec-id");
	if (default_spec_id_val.IsValid()) {
		int32_t default_spec_id_tmp;
		if (json_utils::IsInteger(default_spec_id_val)) {
			default_spec_id_tmp = json_utils::GetSignedInteger(default_spec_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'default_spec_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(default_spec_id_val).c_str());
		}
		default_spec_id = std::move(default_spec_id_tmp);
	}
	auto last_partition_id_val = obj.GetMember("last-partition-id");
	if (last_partition_id_val.IsValid()) {
		int32_t last_partition_id_tmp;
		if (json_utils::IsInteger(last_partition_id_val)) {
			last_partition_id_tmp = json_utils::GetSignedInteger(last_partition_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'last_partition_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(last_partition_id_val).c_str());
		}
		last_partition_id = std::move(last_partition_id_tmp);
	}
	auto sort_orders_val = obj.GetMember("sort-orders");
	if (sort_orders_val.IsValid()) {
		vector<SortOrder> sort_orders_tmp;
		if (sort_orders_val.IsArray()) {
			sort_orders_val.IterateArray([&](JSONValue sort_orders_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				SortOrder sort_orders_tmp_item;
				error = sort_orders_tmp_item.TryFromJSON(sort_orders_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				sort_orders_tmp.emplace_back(std::move(sort_orders_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'sort_orders_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(sort_orders_val).c_str());
		}
		sort_orders = std::move(sort_orders_tmp);
	}
	auto default_sort_order_id_val = obj.GetMember("default-sort-order-id");
	if (default_sort_order_id_val.IsValid()) {
		int32_t default_sort_order_id_tmp;
		if (json_utils::IsInteger(default_sort_order_id_val)) {
			default_sort_order_id_tmp = json_utils::GetSignedInteger(default_sort_order_id_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'default_sort_order_id_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(default_sort_order_id_val).c_str());
		}
		default_sort_order_id = std::move(default_sort_order_id_tmp);
	}
	auto encryption_keys_val = obj.GetMember("encryption-keys");
	if (encryption_keys_val.IsValid()) {
		vector<EncryptedKey> encryption_keys_tmp;
		if (encryption_keys_val.IsArray()) {
			encryption_keys_val.IterateArray([&](JSONValue encryption_keys_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				EncryptedKey encryption_keys_tmp_item;
				error = encryption_keys_tmp_item.TryFromJSON(encryption_keys_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				encryption_keys_tmp.emplace_back(std::move(encryption_keys_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'encryption_keys_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(encryption_keys_val).c_str());
		}
		encryption_keys = std::move(encryption_keys_tmp);
	}
	auto snapshots_val = obj.GetMember("snapshots");
	if (snapshots_val.IsValid()) {
		vector<Snapshot> snapshots_tmp;
		if (snapshots_val.IsArray()) {
			snapshots_val.IterateArray([&](JSONValue snapshots_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				Snapshot snapshots_tmp_item;
				error = snapshots_tmp_item.TryFromJSON(snapshots_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				snapshots_tmp.emplace_back(std::move(snapshots_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format("TableMetadata property 'snapshots_tmp' is not of type 'array', found %s instead",
			                          json_utils::GetTypeDescription(snapshots_val).c_str());
		}
		snapshots = std::move(snapshots_tmp);
	}
	auto refs_val = obj.GetMember("refs");
	if (refs_val.IsValid()) {
		SnapshotReferences refs_tmp;
		error = refs_tmp.TryFromJSON(refs_val);
		if (!error.empty()) {
			return error;
		}
		refs = std::move(refs_tmp);
	}
	auto current_snapshot_id_val = obj.GetMember("current-snapshot-id");
	if (current_snapshot_id_val.IsValid()) {
		if (current_snapshot_id_val.IsNull()) {
			//! do nothing, property is explicitly nullable
		} else {
			int64_t current_snapshot_id_tmp;
			if (json_utils::IsInteger(current_snapshot_id_val)) {
				current_snapshot_id_tmp = json_utils::GetSignedInteger(current_snapshot_id_val);
			} else if (json_utils::IsUnsignedInteger(current_snapshot_id_val)) {
				current_snapshot_id_tmp = json_utils::GetUnsignedInteger(current_snapshot_id_val);
			} else {
				return StringUtil::Format(
				    "TableMetadata property 'current_snapshot_id_tmp' is not of type 'integer', found %s instead",
				    json_utils::GetTypeDescription(current_snapshot_id_val).c_str());
			}
			current_snapshot_id = std::move(current_snapshot_id_tmp);
		}
	}
	auto last_sequence_number_val = obj.GetMember("last-sequence-number");
	if (last_sequence_number_val.IsValid()) {
		int64_t last_sequence_number_tmp;
		if (json_utils::IsInteger(last_sequence_number_val)) {
			last_sequence_number_tmp = json_utils::GetSignedInteger(last_sequence_number_val);
		} else if (json_utils::IsUnsignedInteger(last_sequence_number_val)) {
			last_sequence_number_tmp = json_utils::GetUnsignedInteger(last_sequence_number_val);
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'last_sequence_number_tmp' is not of type 'integer', found %s instead",
			    json_utils::GetTypeDescription(last_sequence_number_val).c_str());
		}
		last_sequence_number = std::move(last_sequence_number_tmp);
	}
	auto snapshot_log_val = obj.GetMember("snapshot-log");
	if (snapshot_log_val.IsValid()) {
		SnapshotLog snapshot_log_tmp;
		error = snapshot_log_tmp.TryFromJSON(snapshot_log_val);
		if (!error.empty()) {
			return error;
		}
		snapshot_log = std::move(snapshot_log_tmp);
	}
	auto metadata_log_val = obj.GetMember("metadata-log");
	if (metadata_log_val.IsValid()) {
		MetadataLog metadata_log_tmp;
		error = metadata_log_tmp.TryFromJSON(metadata_log_val);
		if (!error.empty()) {
			return error;
		}
		metadata_log = std::move(metadata_log_tmp);
	}
	auto statistics_val = obj.GetMember("statistics");
	if (statistics_val.IsValid()) {
		vector<StatisticsFile> statistics_tmp;
		if (statistics_val.IsArray()) {
			statistics_val.IterateArray([&](JSONValue statistics_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				StatisticsFile statistics_tmp_item;
				error = statistics_tmp_item.TryFromJSON(statistics_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				statistics_tmp.emplace_back(std::move(statistics_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'statistics_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(statistics_val).c_str());
		}
		statistics = std::move(statistics_tmp);
	}
	auto partition_statistics_val = obj.GetMember("partition-statistics");
	if (partition_statistics_val.IsValid()) {
		vector<PartitionStatisticsFile> partition_statistics_tmp;
		if (partition_statistics_val.IsArray()) {
			partition_statistics_val.IterateArray([&](JSONValue partition_statistics_tmp_item_val) {
				if (!error.empty()) {
					return;
				}
				PartitionStatisticsFile partition_statistics_tmp_item;
				error = partition_statistics_tmp_item.TryFromJSON(partition_statistics_tmp_item_val);
				if (!error.empty()) {
					return;
				}
				partition_statistics_tmp.emplace_back(std::move(partition_statistics_tmp_item));
			});
			if (!error.empty()) {
				return error;
			}
		} else {
			return StringUtil::Format(
			    "TableMetadata property 'partition_statistics_tmp' is not of type 'array', found %s instead",
			    json_utils::GetTypeDescription(partition_statistics_val).c_str());
		}
		partition_statistics = std::move(partition_statistics_tmp);
	}
	return "";
}

void TableMetadata::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize: format-version
	auto format_version_json = writer.CreateSignedInteger(format_version);
	obj.Add("format-version", format_version_json);

	// Serialize: table-uuid
	auto table_uuid_json = writer.CreateString(table_uuid);
	obj.Add("table-uuid", table_uuid_json);

	// Serialize: location
	if (location.has_value()) {
		auto &location_value = *location;
		auto location_json = writer.CreateString(location_value);
		obj.Add("location", location_json);
	}

	// Serialize: last-updated-ms
	if (last_updated_ms.has_value()) {
		auto &last_updated_ms_value = *last_updated_ms;
		auto last_updated_ms_json = writer.CreateSignedInteger(last_updated_ms_value);
		obj.Add("last-updated-ms", last_updated_ms_json);
	}

	// Serialize: next-row-id
	if (next_row_id.has_value()) {
		auto &next_row_id_value = *next_row_id;
		auto next_row_id_json = writer.CreateSignedInteger(next_row_id_value);
		obj.Add("next-row-id", next_row_id_json);
	}

	// Serialize: properties
	if (properties.has_value()) {
		auto &properties_value = *properties;
		auto properties_json = writer.CreateObject();
		for (const auto &[properties_json_key, properties_json_value] : properties_value) {
			auto properties_json_value_json = writer.CreateString(properties_json_value);
			properties_json.Add(properties_json_key, properties_json_value_json);
		}
		obj.Add("properties", properties_json);
	}

	// Serialize: schemas
	if (schemas.has_value()) {
		auto &schemas_value = *schemas;
		auto schemas_json = writer.CreateArray();
		for (const auto &schemas_json_item : schemas_value) {
			auto schemas_json_item_json = schemas_json_item.ToJSON(writer);
			schemas_json.Append(schemas_json_item_json);
		}
		obj.Add("schemas", schemas_json);
	}

	// Serialize: schema
	if (schema.has_value()) {
		auto &schema_value = *schema;
		auto schema_json = schema_value.ToJSON(writer);
		obj.Add("schema", schema_json);
	}

	// Serialize: current-schema-id
	if (current_schema_id.has_value()) {
		auto &current_schema_id_value = *current_schema_id;
		auto current_schema_id_json = writer.CreateSignedInteger(current_schema_id_value);
		obj.Add("current-schema-id", current_schema_id_json);
	}

	// Serialize: last-column-id
	if (last_column_id.has_value()) {
		auto &last_column_id_value = *last_column_id;
		auto last_column_id_json = writer.CreateSignedInteger(last_column_id_value);
		obj.Add("last-column-id", last_column_id_json);
	}

	// Serialize: partition-specs
	if (partition_specs.has_value()) {
		auto &partition_specs_value = *partition_specs;
		auto partition_specs_json = writer.CreateArray();
		for (const auto &partition_specs_json_item : partition_specs_value) {
			auto partition_specs_json_item_json = partition_specs_json_item.ToJSON(writer);
			partition_specs_json.Append(partition_specs_json_item_json);
		}
		obj.Add("partition-specs", partition_specs_json);
	}

	// Serialize: partition-spec
	if (partition_spec.has_value()) {
		auto &partition_spec_value = *partition_spec;
		auto partition_spec_json = writer.CreateArray();
		for (const auto &partition_spec_json_item : partition_spec_value) {
			auto partition_spec_json_item_json = partition_spec_json_item.ToJSON(writer);
			partition_spec_json.Append(partition_spec_json_item_json);
		}
		obj.Add("partition-spec", partition_spec_json);
	}

	// Serialize: default-spec-id
	if (default_spec_id.has_value()) {
		auto &default_spec_id_value = *default_spec_id;
		auto default_spec_id_json = writer.CreateSignedInteger(default_spec_id_value);
		obj.Add("default-spec-id", default_spec_id_json);
	}

	// Serialize: last-partition-id
	if (last_partition_id.has_value()) {
		auto &last_partition_id_value = *last_partition_id;
		auto last_partition_id_json = writer.CreateSignedInteger(last_partition_id_value);
		obj.Add("last-partition-id", last_partition_id_json);
	}

	// Serialize: sort-orders
	if (sort_orders.has_value()) {
		auto &sort_orders_value = *sort_orders;
		auto sort_orders_json = writer.CreateArray();
		for (const auto &sort_orders_json_item : sort_orders_value) {
			auto sort_orders_json_item_json = sort_orders_json_item.ToJSON(writer);
			sort_orders_json.Append(sort_orders_json_item_json);
		}
		obj.Add("sort-orders", sort_orders_json);
	}

	// Serialize: default-sort-order-id
	if (default_sort_order_id.has_value()) {
		auto &default_sort_order_id_value = *default_sort_order_id;
		auto default_sort_order_id_json = writer.CreateSignedInteger(default_sort_order_id_value);
		obj.Add("default-sort-order-id", default_sort_order_id_json);
	}

	// Serialize: encryption-keys
	if (encryption_keys.has_value()) {
		auto &encryption_keys_value = *encryption_keys;
		auto encryption_keys_json = writer.CreateArray();
		for (const auto &encryption_keys_json_item : encryption_keys_value) {
			auto encryption_keys_json_item_json = encryption_keys_json_item.ToJSON(writer);
			encryption_keys_json.Append(encryption_keys_json_item_json);
		}
		obj.Add("encryption-keys", encryption_keys_json);
	}

	// Serialize: snapshots
	if (snapshots.has_value()) {
		auto &snapshots_value = *snapshots;
		auto snapshots_json = writer.CreateArray();
		for (const auto &snapshots_json_item : snapshots_value) {
			auto snapshots_json_item_json = snapshots_json_item.ToJSON(writer);
			snapshots_json.Append(snapshots_json_item_json);
		}
		obj.Add("snapshots", snapshots_json);
	}

	// Serialize: refs
	if (refs.has_value()) {
		auto &refs_value = *refs;
		auto refs_json = refs_value.ToJSON(writer);
		obj.Add("refs", refs_json);
	}

	// Serialize: current-snapshot-id
	if (current_snapshot_id.has_value()) {
		auto &current_snapshot_id_value = *current_snapshot_id;
		auto current_snapshot_id_json = writer.CreateSignedInteger(current_snapshot_id_value);
		obj.Add("current-snapshot-id", current_snapshot_id_json);
	}

	// Serialize: last-sequence-number
	if (last_sequence_number.has_value()) {
		auto &last_sequence_number_value = *last_sequence_number;
		auto last_sequence_number_json = writer.CreateSignedInteger(last_sequence_number_value);
		obj.Add("last-sequence-number", last_sequence_number_json);
	}

	// Serialize: snapshot-log
	if (snapshot_log.has_value()) {
		auto &snapshot_log_value = *snapshot_log;
		auto snapshot_log_json = snapshot_log_value.ToJSON(writer);
		obj.Add("snapshot-log", snapshot_log_json);
	}

	// Serialize: metadata-log
	if (metadata_log.has_value()) {
		auto &metadata_log_value = *metadata_log;
		auto metadata_log_json = metadata_log_value.ToJSON(writer);
		obj.Add("metadata-log", metadata_log_json);
	}

	// Serialize: statistics
	if (statistics.has_value()) {
		auto &statistics_value = *statistics;
		auto statistics_json = writer.CreateArray();
		for (const auto &statistics_json_item : statistics_value) {
			auto statistics_json_item_json = statistics_json_item.ToJSON(writer);
			statistics_json.Append(statistics_json_item_json);
		}
		obj.Add("statistics", statistics_json);
	}

	// Serialize: partition-statistics
	if (partition_statistics.has_value()) {
		auto &partition_statistics_value = *partition_statistics;
		auto partition_statistics_json = writer.CreateArray();
		for (const auto &partition_statistics_json_item : partition_statistics_value) {
			auto partition_statistics_json_item_json = partition_statistics_json_item.ToJSON(writer);
			partition_statistics_json.Append(partition_statistics_json_item_json);
		}
		obj.Add("partition-statistics", partition_statistics_json);
	}
}

JSONMutableValue TableMetadata::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
