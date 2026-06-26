#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/api/iceberg_table_requirement.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "core/metadata/sort/iceberg_sort_order.hpp"

namespace duckdb {

struct IcebergTableInformation;

struct AddSchemaUpdate : public IcebergTableUpdate {
public:
	explicit AddSchemaUpdate(const IcebergTableInformation &table_info, const IcebergTableMetadata &table_metadata,
	                         int32_t schema_id);

public:
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SCHEMA;

	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

public:
	int32_t schema_id;
	optional_idx last_column_id;
	string schema_json;
};

struct AssertCreateRequirement : public IcebergTableRequirement {
	static constexpr const IcebergTableRequirementType TYPE = IcebergTableRequirementType::ASSERT_CREATE;

	explicit AssertCreateRequirement(const IcebergTableInformation &table_info);
	void CreateRequirement(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
};

struct AssertTableUUIDRequirement : public IcebergTableRequirement {
	static constexpr const IcebergTableRequirementType TYPE = IcebergTableRequirementType::ASSERT_TABLE_UUID;

	explicit AssertTableUUIDRequirement(const IcebergTableInformation &table_info);
	void CreateRequirement(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);

	string uuid;
};

struct AssertCurrentSchemaIdRequirement : public IcebergTableRequirement {
	static constexpr const IcebergTableRequirementType TYPE = IcebergTableRequirementType::ASSERT_CURRENT_SCHEMA_ID;

	explicit AssertCurrentSchemaIdRequirement(const IcebergTableInformation &table_info);
	void CreateRequirement(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);

	int32_t current_schema_id;
};

//! The table's last assigned column id (a.k.a last_column_id) must match the requirement's `last-assigned-field-id`
struct AssertLastAssignedFieldIdRequirement : public IcebergTableRequirement {
	static constexpr const IcebergTableRequirementType TYPE =
	    IcebergTableRequirementType::ASSERT_LAST_ASSIGNED_FIELD_ID;

	explicit AssertLastAssignedFieldIdRequirement(const IcebergTableInformation &table_info);
	void CreateRequirement(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);

	int32_t last_assigned_field_id;
};

struct AssertLastAssignedPartitionIdRequirement : public IcebergTableRequirement {
	static constexpr const IcebergTableRequirementType TYPE =
	    IcebergTableRequirementType::ASSERT_LAST_ASSIGNED_PARTITION_ID;

	explicit AssertLastAssignedPartitionIdRequirement(const IcebergTableInformation &table_info);
	void CreateRequirement(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);

	int32_t last_assigned_partition_id;
};

struct AssertDefaultSpecIdRequirement : public IcebergTableRequirement {
	static constexpr const IcebergTableRequirementType TYPE = IcebergTableRequirementType::ASSERT_DEFAULT_SPEC_ID;

	explicit AssertDefaultSpecIdRequirement(const IcebergTableInformation &table_info);
	void CreateRequirement(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);

	int32_t default_spec_id;
};

struct AssignUUIDUpdate : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ASSIGN_UUID;

	explicit AssignUUIDUpdate(string uuid);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

	string uuid;
};

struct UpgradeFormatVersion : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::UPGRADE_FORMAT_VERSION;

	explicit UpgradeFormatVersion(int32_t format_version);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

	int32_t format_version;
};

struct SetCurrentSchema : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_CURRENT_SCHEMA;

	explicit SetCurrentSchema(int32_t schema_id);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

	int32_t schema_id;
};

struct AddPartitionSpec : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_PARTITION_SPEC;
	explicit AddPartitionSpec(const IcebergPartitionSpec &partition_spec);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

	IcebergPartitionSpec partition_spec;
};

struct AddSortOrder : public IcebergTableUpdate {
	static constexpr const int64_t DEFAULT_SORT_ORDER_ID = 0;
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SORT_ORDER;

	explicit AddSortOrder(const IcebergSortOrder &sort_order);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

	IcebergSortOrder sort_order;
};

struct SetDefaultSortOrder : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_DEFAULT_SORT_ORDER;

	explicit SetDefaultSortOrder(int32_t sort_order_id);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

	int32_t sort_order_id;
};

struct SetDefaultSpec : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_DEFAULT_SPEC;

	explicit SetDefaultSpec(int32_t spec_id);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

	int32_t spec_id;
};

struct SetProperties : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_PROPERTIES;

	explicit SetProperties(const IcebergTableInformation &table_info, const case_insensitive_map_t<string> &properties);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

	case_insensitive_map_t<string> properties;
};

struct RemoveProperties : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::REMOVE_PROPERTIES;

	explicit RemoveProperties(const IcebergTableInformation &table_info, const vector<string> &properties);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

	vector<string> properties;
};

struct SetLocation : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_LOCATION;

	explicit SetLocation(string location);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

	string location;
};

} // namespace duckdb
