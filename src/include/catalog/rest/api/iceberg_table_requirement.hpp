#pragma once

#include "duckdb/main/database.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

struct IcebergCommitState;

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
	explicit IcebergTableRequirement(IcebergTableRequirementType type) : type(type) {
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
};

} // namespace duckdb
