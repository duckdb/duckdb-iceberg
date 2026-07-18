#include "catalog/rest/api/table_update.hpp"
#include "duckdb/common/exception.hpp"
#include "catalog/rest/iceberg_table_set.hpp"

namespace duckdb {

AddSchemaUpdate::AddSchemaUpdate(shared_ptr<IcebergTableSchema> schema_p, optional_idx last_column_id_p)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SCHEMA), last_column_id(last_column_id_p),
      schema(std::move(schema_p)) {
	if (!schema) {
		throw InternalException("(AddSchemaUpdate) Missing schema payload");
	}
}

void AddSchemaUpdate::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                   IcebergCommitState &commit_state) const {
	(void)db;
	(void)context;
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &update = commit_state.table_change.updates.back();
	update.add_schema_update = rest_api_objects::AddSchemaUpdate();
	update.add_schema_update->base_update.action = "add-schema";
	update.add_schema_update->schema = schema->ToRESTObject();
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

AssertCreateRequirement::AssertCreateRequirement()
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_CREATE) {
}

void AssertCreateRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_create = rest_api_objects::AssertCreate();
	req.assert_create->type = "assert-create";
}

AssertTableUUIDRequirement::AssertTableUUIDRequirement(string uuid)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_TABLE_UUID), uuid(std::move(uuid)) {
}

void AssertTableUUIDRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                   IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_table_uuid = rest_api_objects::AssertTableUUID();
	req.assert_table_uuid->type = "assert-table-uuid";
	req.assert_table_uuid->uuid = uuid;
}

AssertCurrentSchemaIdRequirement::AssertCurrentSchemaIdRequirement(int32_t current_schema_id)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_CURRENT_SCHEMA_ID),
      current_schema_id(current_schema_id) {
}

void AssertCurrentSchemaIdRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                         IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_current_schema_id = rest_api_objects::AssertCurrentSchemaId();
	req.assert_current_schema_id->type = "assert-current-schema-id";
	req.assert_current_schema_id->current_schema_id = current_schema_id;
}

AssertRefSnapshotId::AssertRefSnapshotId(int64_t snapshot_id)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_REF_SNAPSHOT_ID), snapshot_id(snapshot_id) {
}

void AssertRefSnapshotId::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                            IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_ref_snapshot_id = rest_api_objects::AssertRefSnapshotId();
	req.assert_ref_snapshot_id->type = "assert-ref-snapshot-id";
	req.assert_ref_snapshot_id->ref = "main";
	req.assert_ref_snapshot_id->snapshot_id = snapshot_id;
}

AssertLastAssignedFieldIdRequirement::AssertLastAssignedFieldIdRequirement(int32_t last_assigned_field_id)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_LAST_ASSIGNED_FIELD_ID),
      last_assigned_field_id(last_assigned_field_id) {
}

void AssertLastAssignedFieldIdRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                             IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_last_assigned_field_id = rest_api_objects::AssertLastAssignedFieldId();
	req.assert_last_assigned_field_id->type = "assert-last-assigned-field-id";
	req.assert_last_assigned_field_id->last_assigned_field_id = last_assigned_field_id;
}

AssertLastAssignedPartitionIdRequirement::AssertLastAssignedPartitionIdRequirement(int32_t last_assigned_partition_id)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_LAST_ASSIGNED_PARTITION_ID),
      last_assigned_partition_id(last_assigned_partition_id) {
}

void AssertLastAssignedPartitionIdRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                                 IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_last_assigned_partition_id = rest_api_objects::AssertLastAssignedPartitionId();
	req.assert_last_assigned_partition_id->type = "assert-last-assigned-partition-id";
	req.assert_last_assigned_partition_id->last_assigned_partition_id = last_assigned_partition_id;
}

AssertDefaultSpecIdRequirement::AssertDefaultSpecIdRequirement(int32_t default_spec_id)
    : IcebergTableRequirement(IcebergTableRequirementType::ASSERT_DEFAULT_SPEC_ID), default_spec_id(default_spec_id) {
}

void AssertDefaultSpecIdRequirement::CreateRequirement(DatabaseInstance &db, ClientContext &context,
                                                       IcebergCommitState &commit_state) {
	commit_state.table_change.requirements.push_back(rest_api_objects::TableRequirement());
	auto &req = commit_state.table_change.requirements.back();
	req.assert_default_spec_id = rest_api_objects::AssertDefaultSpecId();
	req.assert_default_spec_id->type = "assert-default-spec-id";
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

SetProperties::SetProperties(const case_insensitive_map_t<string> &properties)
    : IcebergTableUpdate(IcebergTableUpdateType::SET_PROPERTIES), properties(properties) {
}

void SetProperties::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const {
	commit_state.table_change.updates.push_back(rest_api_objects::TableUpdate());
	auto &req = commit_state.table_change.updates.back();
	req.set_properties_update = rest_api_objects::SetPropertiesUpdate();
	req.set_properties_update->base_update.action = "set-properties";
	req.set_properties_update->updates = properties;
}

RemoveProperties::RemoveProperties(const vector<string> &properties)
    : IcebergTableUpdate(IcebergTableUpdateType::REMOVE_PROPERTIES), properties(properties) {
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
