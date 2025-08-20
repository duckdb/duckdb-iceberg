#pragma once

#include "rest_catalog/objects/list.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

struct IcebergTableInformation;

enum class IcebergTableRequirementType : uint8_t {
	ASSERT_CREATE,
	ASSERT_TABLE_UUID,
	ASSERT_REF_SNAPSHOT_ID,
	ASSERT_LAST_ASSIGNED_FIELD_ID,
	ASSERT_CURRENT_SCHEMA_ID,
	ASSERT_LAST_ASSIGNED_PARTITION_ID,
	ASSERT_DEFAULT_SPEC_ID,
	ASSERT_DEFAULT_SORT_ORDER_ID,
};

struct IcebergTableRequirement {
public:
	IcebergTableRequirement(IcebergTableRequirementType type, IcebergTableInformation &table_info)
	    : type(type), table_info(table_info) {
	}
	virtual ~IcebergTableRequirement() {
	}

public:
	virtual void CreateRequirement(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) = 0;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast IcebergTableRequirement to type - type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

public:
	IcebergTableRequirementType type;
	IcebergTableInformation &table_info;
};

} // namespace duckdb
