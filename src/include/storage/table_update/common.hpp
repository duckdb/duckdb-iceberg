#pragma once

#include "storage/iceberg_table_update.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_snapshot.hpp"

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "storage/iceberg_table_requirement.hpp"
#include "duckdb/common/types/value.hpp"
#include "metadata/iceberg_table_schema.hpp"

namespace duckdb {

struct IcebergTableInformation;

struct AddSchemaUpdate : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SCHEMA;

	explicit AddSchemaUpdate(const IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

	optional_ptr<const IcebergTableSchema> table_schema = nullptr;
};

struct AssertCreateRequirement : public IcebergTableRequirement {
	static constexpr const IcebergTableRequirementType TYPE = IcebergTableRequirementType::ASSERT_CREATE;

	explicit AssertCreateRequirement(const IcebergTableInformation &table_info);
	void CreateRequirement(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
};

struct AssignUUIDUpdate : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ASSIGN_UUID;

	explicit AssignUUIDUpdate(const IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;
};

struct UpgradeFormatVersion : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::UPGRADE_FORMAT_VERSION;

	explicit UpgradeFormatVersion(const IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;
};

struct SetCurrentSchema : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::UPGRADE_FORMAT_VERSION;

	explicit SetCurrentSchema(const IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;
};

struct AddPartitionSpec : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::UPGRADE_FORMAT_VERSION;

	explicit AddPartitionSpec(const IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;
};

struct AddSortOrder : public IcebergTableUpdate {
	static constexpr const int64_t DEFAULT_SORT_ORDER_ID = 0;
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SORT_ORDER;

	explicit AddSortOrder(const IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;
};

struct SetDefaultSortOrder : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_DEFAULT_SORT_ORDER;

	explicit SetDefaultSortOrder(const IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;
};

struct SetDefaultSpec : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_DEFAULT_SPEC;

	explicit SetDefaultSpec(const IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;
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

	explicit SetLocation(const IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;
};

} // namespace duckdb
