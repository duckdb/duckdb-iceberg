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

	explicit AddSchemaUpdate(IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);

	optional_ptr<IcebergTableSchema> table_schema = nullptr;
};

struct AssertCreateRequirement : public IcebergTableRequirement {
	static constexpr const IcebergTableRequirementType TYPE = IcebergTableRequirementType::ASSERT_CREATE;

	explicit AssertCreateRequirement(IcebergTableInformation &table_info);
	void CreateRequirement(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
};

struct AssignUUIDUpdate : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ASSIGN_UUID;

	explicit AssignUUIDUpdate(IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
};

struct UpgradeFormatVersion : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::UPGRADE_FORMAT_VERSION;

	explicit UpgradeFormatVersion(IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
};

struct SetCurrentSchema : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::UPGRADE_FORMAT_VERSION;

	explicit SetCurrentSchema(IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
};

struct AddPartitionSpec : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::UPGRADE_FORMAT_VERSION;

	explicit AddPartitionSpec(IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
};

struct AddSortOrder : public IcebergTableUpdate {
	static constexpr const int64_t DEFAULT_SORT_ORDER_ID = 0;
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SORT_ORDER;

	explicit AddSortOrder(IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
};

struct SetDefaultSortOrder : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_DEFAULT_SORT_ORDER;

	explicit SetDefaultSortOrder(IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
};

struct SetDefaultSpec : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_DEFAULT_SPEC;

	explicit SetDefaultSpec(IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
};

struct SetProperties : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_PROPERTIES;

	explicit SetProperties(IcebergTableInformation &table_info, case_insensitive_map_t<string> properties);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);

	case_insensitive_map_t<string> properties;
};

struct RemoveProperties : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::REMOVE_PROPERTIES;

	explicit RemoveProperties(IcebergTableInformation &table_info, vector<string> properties);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);

	vector<string> properties;
};

struct SetLocation : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::SET_LOCATION;

	explicit SetLocation(IcebergTableInformation &table_info);
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state);
};

} // namespace duckdb
