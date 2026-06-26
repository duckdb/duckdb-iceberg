#include "catalog/rest/api/table_update.hpp"
#include "duckdb/common/exception.hpp"
#include "catalog/rest/iceberg_table_set.hpp"

namespace duckdb {

AddSchemaUpdate::AddSchemaUpdate(const IcebergTableInformation &table_info, const IcebergTableMetadata &table_metadata,
                                 int32_t schema_id)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SCHEMA), schema_id(schema_id) {
	if (table_metadata.HasLastColumnId()) {
		last_column_id = table_metadata.GetLastColumnId();
	}

	auto &schemas = table_metadata.GetSchemas();
	auto it = schemas.find(schema_id);
	if (it == schemas.end()) {
		throw InternalException("(AddSchemaUpdate) Couldn't find schema with id: %d", schema_id);
	}
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	yyjson_mut_doc *doc = doc_p.get();
	auto root_object = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_object);
	IcebergCreateTableRequest::PopulateSchema(doc, root_object, *it->second);
	schema_json = ICUtils::JsonToString(std::move(doc_p));
}

void AddSchemaUpdate::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                   IcebergCommitState &commit_state) const {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &update = commit_state.table_change.updates.back();
	update.add_schema_update = rest_api_objects::AddSchemaUpdate();
	update.add_schema_update->base_update.action = "add-schema";

	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> new_doc(yyjson_read(schema_json.c_str(), schema_json.size(), 0));
	yyjson_val *val = yyjson_doc_get_root(new_doc.get());
	update.add_schema_update->schema = rest_api_objects::Schema::FromJSON(val);
	// last-column-id is technically deprecated in AddSchemaUpdate, but some catalogs still use it (nessie).
	if (last_column_id.IsValid()) {
		update.add_schema_update->last_column_id = last_column_id.GetIndex();
	}
}

AssignUUIDUpdate::AssignUUIDUpdate(string uuid)
    : IcebergTableUpdate(IcebergTableUpdateType::ASSIGN_UUID), uuid(std::move(uuid)) {
}

void AssignUUIDUpdate::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                    IcebergCommitState &commit_state) const {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &update = commit_state.table_change.updates.back();
	update.assign_uuidupdate = rest_api_objects::AssignUUIDUpdate();
	update.assign_uuidupdate->base_update.action = "assign-uuid";
	// uuid most likely created by the rest catalog?
	update.assign_uuidupdate->uuid = uuid;
}

AssertCreateRequirement::AssertCreateRequirement(const IcebergTableInformation &table_info)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_CREATE) {
}

void AssertCreateRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_create = rest_api_objects::AssertCreate();
	req.assert_create->type.value = "assert-create";
}

AssertTableUUIDRequirement::AssertTableUUIDRequirement(const IcebergTableInformation &table_info)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_TABLE_UUID),
      uuid(table_info.table_metadata.table_uuid) {
}

void AssertTableUUIDRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                   IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_table_uuid = rest_api_objects::AssertTableUUID();
	req.assert_table_uuid->type.value = "assert-table-uuid";
	req.assert_table_uuid->uuid = uuid;
}

AssertCurrentSchemaIdRequirement::AssertCurrentSchemaIdRequirement(const IcebergTableInformation &table_info)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_CURRENT_SCHEMA_ID) {
	current_schema_id = table_info.table_metadata.GetCurrentSchemaId();
}

void AssertCurrentSchemaIdRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                         IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_current_schema_id = rest_api_objects::AssertCurrentSchemaId();
	req.assert_current_schema_id->type.value = "assert-current-schema-id";
	req.assert_current_schema_id->current_schema_id = current_schema_id;
}

AssertLastAssignedFieldIdRequirement::AssertLastAssignedFieldIdRequirement(const IcebergTableInformation &table_info)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_LAST_ASSIGNED_FIELD_ID) {
	D_ASSERT(table_info.table_metadata.HasLastColumnId());
	last_assigned_field_id = static_cast<int32_t>(table_info.table_metadata.GetLastColumnId());
}

void AssertLastAssignedFieldIdRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                             IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_last_assigned_field_id = rest_api_objects::AssertLastAssignedFieldId();
	req.assert_last_assigned_field_id->type.value = "assert-last-assigned-field-id";
	req.assert_last_assigned_field_id->last_assigned_field_id = last_assigned_field_id;
}

AssertLastAssignedPartitionIdRequirement::AssertLastAssignedPartitionIdRequirement(
    const IcebergTableInformation &table_info)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_LAST_ASSIGNED_PARTITION_ID) {
	if (table_info.table_metadata.HasLastPartitionId()) {
		last_assigned_partition_id = table_info.table_metadata.GetLastPartitionFieldId();
	} else {
		// If no partition field IDs have been assigned, use 999 as the last assigned so 1000 becomes the
		// next partition id. Based on assignments in v1 in https://iceberg.apache.org/spec/#partition-evolution
		last_assigned_partition_id = 999;
	}
}

void AssertLastAssignedPartitionIdRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                                 IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_last_assigned_partition_id = rest_api_objects::AssertLastAssignedPartitionId();
	req.assert_last_assigned_partition_id->type.value = "assert-last-assigned-partition-id";
	req.assert_last_assigned_partition_id->last_assigned_partition_id = last_assigned_partition_id;
}

AssertDefaultSpecIdRequirement::AssertDefaultSpecIdRequirement(const IcebergTableInformation &table_info)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_DEFAULT_SPEC_ID) {
	default_spec_id = table_info.table_metadata.default_spec_id;
}

void AssertDefaultSpecIdRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                       IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_default_spec_id = rest_api_objects::AssertDefaultSpecId();
	req.assert_default_spec_id->type.value = "assert-default-spec-id";
	req.assert_default_spec_id->default_spec_id = default_spec_id;
}

UpgradeFormatVersion::UpgradeFormatVersion(int32_t format_version)
    : IcebergTableUpdate(IcebergTableUpdateType::UPGRADE_FORMAT_VERSION), format_version(format_version) {
}

void UpgradeFormatVersion::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                        IcebergCommitState &commit_state) const {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.upgrade_format_version_update = rest_api_objects::UpgradeFormatVersionUpdate();
	req.upgrade_format_version_update->base_update.action = "upgrade-format-version";
	req.upgrade_format_version_update->format_version = format_version;
}

SetCurrentSchema::SetCurrentSchema(int32_t schema_id)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_CURRENT_SCHEMA), schema_id(schema_id) {
}

void SetCurrentSchema::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                    IcebergCommitState &commit_state) const {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.set_current_schema_update = rest_api_objects::SetCurrentSchemaUpdate();
	req.set_current_schema_update->base_update.action = "set-current-schema";
	// TODO: should this be a different value? or is the rest catalog setting this again?
	req.set_current_schema_update->schema_id = schema_id;
}

AddPartitionSpec::AddPartitionSpec(const IcebergPartitionSpec &partition_spec)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_PARTITION_SPEC), partition_spec(partition_spec) {
}

void AddPartitionSpec::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                    IcebergCommitState &commit_state) const {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.add_partition_spec_update = rest_api_objects::AddPartitionSpecUpdate();
	req.add_partition_spec_update->base_update.action = "add-spec";
	// need to get the spec id from table_info() so we can also check updated tables.
	req.add_partition_spec_update->spec.spec_id = partition_spec.spec_id;
	for (auto &field : partition_spec.fields) {
		req.add_partition_spec_update->spec.fields.push_back(rest_api_objects::PartitionField());
		auto &updated_field = req.add_partition_spec_update->spec.fields.back();
		updated_field.name = field.GetPartitionSpecFieldName();
		updated_field.transform.value = field.transform.RawType();
		updated_field.field_id = field.partition_field_id;
		updated_field.source_id = field.source_id;
	}
}

AddSortOrder::AddSortOrder(const IcebergSortOrder &sort_order)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SORT_ORDER), sort_order(sort_order) {
}

void AddSortOrder::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.add_sort_order_update = rest_api_objects::AddSortOrderUpdate();
	req.add_sort_order_update->base_update.action = "add-sort-order";
	req.add_sort_order_update->sort_order.order_id = sort_order.sort_order_id;
	for (auto &field : sort_order.fields) {
		req.add_sort_order_update->sort_order.fields.push_back(rest_api_objects::SortField());
		auto &updated_field = req.add_sort_order_update->sort_order.fields.back();
		updated_field.direction.value = field.direction;
		updated_field.transform.value = field.transform.RawType();
		updated_field.null_order.value = field.null_order;
		updated_field.source_id = field.source_id;
	}
}

SetDefaultSortOrder::SetDefaultSortOrder(int32_t sort_order_id)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_DEFAULT_SORT_ORDER), sort_order_id(sort_order_id) {
}

void SetDefaultSortOrder::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                       IcebergCommitState &commit_state) const {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.set_default_sort_order_update = rest_api_objects::SetDefaultSortOrderUpdate();
	req.set_default_sort_order_update->base_update.action = "set-default-sort-order";
	req.set_default_sort_order_update->sort_order_id = sort_order_id;
}

SetDefaultSpec::SetDefaultSpec(int32_t spec_id)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_DEFAULT_SPEC), spec_id(spec_id) {
}

void SetDefaultSpec::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                  IcebergCommitState &commit_state) const {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.set_default_spec_update = rest_api_objects::SetDefaultSpecUpdate();
	req.set_default_spec_update->base_update.action = "set-default-spec";
	req.set_default_spec_update->spec_id = spec_id;
}

SetProperties::SetProperties(const IcebergTableInformation &table_info,
                             const case_insensitive_map_t<string> &properties)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_PROPERTIES), properties(properties) {
}

void SetProperties::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.set_properties_update = rest_api_objects::SetPropertiesUpdate();
	req.set_properties_update->base_update.action = "set-properties";
	req.set_properties_update->updates = properties;
}

RemoveProperties::RemoveProperties(const IcebergTableInformation &table_info, const vector<string> &properties)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_PROPERTIES), properties(properties) {
}

void RemoveProperties::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                    IcebergCommitState &commit_state) const {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.remove_properties_update = rest_api_objects::RemovePropertiesUpdate();
	req.remove_properties_update->base_update.action = "remove-properties";
	req.remove_properties_update->removals = properties;
}

SetLocation::SetLocation(string location)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_LOCATION), location(std::move(location)) {
}

void SetLocation::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.set_location_update = rest_api_objects::SetLocationUpdate();
	req.set_location_update->base_update.action = "set-location";
	req.set_location_update->location = location;
}

} // namespace duckdb
