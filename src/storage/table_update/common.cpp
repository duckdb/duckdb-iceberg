#include "storage/table_update/common.hpp"
#include "storage/irc_table_set.hpp"

namespace duckdb {

static rest_api_objects::Schema CopySchema(IcebergTableSchema &schema) {
	// the rest api objects are currently not copyable. Without having to modify generated code
	//  the easiest way to copy for now is to write the schema to string, then parse it again
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	yyjson_mut_doc *doc = doc_p.get();
	auto root_object = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_object);
	IcebergCreateTableRequest::PopulateSchema(doc, root_object, schema);
	auto schema_str = ICUtils::JsonToString(std::move(doc_p));

	// Parse it back as immutable
	yyjson_doc *new_doc = yyjson_read(schema_str.c_str(), strlen(schema_str.c_str()), 0);
	yyjson_val *val = yyjson_doc_get_root(new_doc);
	return rest_api_objects::Schema::FromJSON(val);
}

AddSchemaUpdate::AddSchemaUpdate(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SCHEMA, table_info) {
	auto current_schema_id = table_info.table_metadata.current_schema_id;
	if (table_info.table_metadata.schemas.find(current_schema_id) == table_info.table_metadata.schemas.end()) {
		throw InvalidConfigurationException("cannot assign a current schema id for a schema that does not yet exist");
	};
	table_schema = table_info.table_metadata.schemas[current_schema_id];
}

void AddSchemaUpdate::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &update = commit_state.table_change.updates.back();
	update.has_add_schema_update = true;
	update.add_schema_update.has_action = true;
	update.add_schema_update.action = "add-schema";
	auto current_schema_id = table_info.load_table_result.metadata.current_schema_id;
	auto &schema = table_info.table_metadata.schemas[current_schema_id];
	update.add_schema_update.schema = CopySchema(*schema.get());
	// last column id is technically deprecated, but some catalogs still use it (nessie).
	if (table_info.load_table_result.metadata.has_last_column_id) {
		update.add_schema_update.has_last_column_id = true;
		update.add_schema_update.last_column_id = table_info.load_table_result.metadata.last_column_id;
	}
}

AssignUUIDUpdate::AssignUUIDUpdate(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SCHEMA, table_info) {
}

void AssignUUIDUpdate::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &update = commit_state.table_change.updates.back();
	update.has_assign_uuidupdate = true;
	update.assign_uuidupdate.action = "assign-uuid";
	update.assign_uuidupdate.has_action = true;
	// uuid most likely created by the rest catalog?
	update.assign_uuidupdate.uuid = table_info.table_metadata.table_uuid;
}

AssertCreateRequirement::AssertCreateRequirement(IcebergTableInformation &table_info)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_CREATE, table_info) {
}

void AssertCreateRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_create.type.value = "assert-create";
	req.has_assert_create = true;
}

UpgradeFormatVersion::UpgradeFormatVersion(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::UPGRADE_FORMAT_VERSION, table_info) {
}

void UpgradeFormatVersion::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                        IcebergCommitState &commit_state) {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.has_upgrade_format_version_update = true;
	req.upgrade_format_version_update.action = "upgrade-format-version";
	req.upgrade_format_version_update.has_action = true;
	req.upgrade_format_version_update.format_version = table_info.table_metadata.iceberg_version;
}

SetCurrentSchema::SetCurrentSchema(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_CURRENT_SCHEMA, table_info) {
}

void SetCurrentSchema::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.has_set_current_schema_update = true;
	req.set_current_schema_update.action = "set-current-schema";
	// TODO: should this be a different value? or is the rest catalog setting this again?
	req.set_current_schema_update.schema_id = table_info.table_metadata.current_schema_id;
}

AddPartitionSpec::AddPartitionSpec(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_PARTITION_SPEC, table_info) {
}

void AddPartitionSpec::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.has_add_partition_spec_update = true;
	req.add_partition_spec_update.has_action = true;
	req.add_partition_spec_update.action = "add-spec";
	req.add_partition_spec_update.spec.has_spec_id = true;
	req.add_partition_spec_update.spec.spec_id = table_info.table_metadata.default_spec_id;
	idx_t partition_spec_id = req.add_partition_spec_update.spec.spec_id;
	if (table_info.load_table_result.metadata.has_partition_specs) {
		auto &table_partition_specs = table_info.load_table_result.metadata.partition_specs;
		optional_ptr<rest_api_objects::PartitionSpec> current_partition_spect = nullptr;
		for (auto &partition_spec : table_partition_specs) {
			if (partition_spec.spec_id == partition_spec_id) {
				current_partition_spect = partition_spec;
			}
		}
		for (auto &field : current_partition_spect.get()->fields) {
			req.add_partition_spec_update.spec.fields.push_back(rest_api_objects::PartitionField());
			auto &updated_field = req.add_partition_spec_update.spec.fields.back();
			updated_field.name = field.name;
			updated_field.transform.value = field.transform.value;
			updated_field.field_id = field.field_id;
			updated_field.source_id = field.source_id;
			updated_field.has_field_id = field.has_field_id;
		}
	}
}

AddSortOrder::AddSortOrder(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SORT_ORDER, table_info) {
}

void AddSortOrder::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.has_add_sort_order_update = true;
	req.add_sort_order_update.has_action = true;
	req.add_sort_order_update.action = "add-sort-order";
	idx_t sort_order_id = 0;
	if (table_info.load_table_result.metadata.has_default_sort_order_id) {
		req.add_sort_order_update.sort_order.order_id = table_info.load_table_result.metadata.default_sort_order_id;
		sort_order_id = req.add_sort_order_update.sort_order.order_id;
	}

	if (table_info.load_table_result.metadata.has_sort_orders) {
		auto &table_sort_orders = table_info.load_table_result.metadata.sort_orders;
		optional_ptr<rest_api_objects::SortOrder> current_sort_order = nullptr;
		for (auto &sort_order : table_sort_orders) {
			if (sort_order.order_id == sort_order_id) {
				current_sort_order = sort_order;
			}
		}
		for (auto &field : current_sort_order.get()->fields) {
			req.add_sort_order_update.sort_order.fields.push_back(rest_api_objects::SortField());
			auto &updated_field = req.add_sort_order_update.sort_order.fields.back();
			updated_field.direction.value = field.direction.value;
			updated_field.transform.value = field.transform.value;
			updated_field.null_order.value = field.null_order.value;
			updated_field.source_id = field.source_id;
		}
	}
}

SetDefaultSortOrder::SetDefaultSortOrder(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_DEFAULT_SORT_ORDER, table_info) {
}

void SetDefaultSortOrder::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.has_set_default_sort_order_update = true;
	req.set_default_sort_order_update.has_action = true;
	req.set_default_sort_order_update.action = "set-default-sort-order";
	D_ASSERT(table_info.load_table_result.metadata.has_default_sort_order_id);
	req.set_default_sort_order_update.sort_order_id = table_info.load_table_result.metadata.default_sort_order_id;
}

SetDefaultSpec::SetDefaultSpec(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_DEFAULT_SPEC, table_info) {
}

void SetDefaultSpec::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.has_set_default_spec_update = true;
	req.set_default_spec_update.has_action = true;
	req.set_default_spec_update.action = "set-default-spec";
	req.set_default_spec_update.spec_id = 0;
}

SetProperties::SetProperties(IcebergTableInformation &table_info, case_insensitive_map_t<string> properties)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_PROPERTIES, table_info), properties(properties) {
}

void SetProperties::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.has_set_properties_update = true;
	req.set_properties_update.action = "set-properties";
	req.set_properties_update.updates = properties;
}

RemoveProperties::RemoveProperties(IcebergTableInformation &table_info, vector<string> properties)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_PROPERTIES, table_info), properties(properties) {
}

void RemoveProperties::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.has_remove_properties_update = true;
	req.remove_properties_update.action = "remove-properties";
	req.remove_properties_update.removals = properties;
}

SetLocation::SetLocation(IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_LOCATION, table_info) {
}

void SetLocation::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.has_set_location_update = true;
	req.set_location_update.action = "set-location";
	req.set_location_update.location = table_info.table_metadata.location;
}

} // namespace duckdb
