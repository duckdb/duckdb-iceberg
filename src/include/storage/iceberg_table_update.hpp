#pragma once

#include "rest_catalog/objects/list.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "metadata/iceberg_manifest_list.hpp"

namespace duckdb {

struct IcebergTableInformation;
struct IcebergTransactionData;

enum class IcebergTableUpdateType : uint8_t {
	ASSIGN_UUID,
	UPGRADE_FORMAT_VERSION,
	ADD_SCHEMA,
	SET_CURRENT_SCHEMA,
	ADD_PARTITION_SPEC,
	SET_DEFAULT_SPEC,
	ADD_SORT_ORDER,
	SET_DEFAULT_SORT_ORDER,
	ADD_SNAPSHOT,
	SET_SNAPSHOT_REF,
	REMOVE_SNAPSHOTS,
	REMOVE_SNAPSHOT_REF,
	SET_LOCATION,
	SET_PROPERTIES,
	REMOVE_PROPERTIES,
	SET_STATISTICS,
	REMOVE_STATISTICS,
	REMOVE_PARTITION_SPECS,
	REMOVE_SCHEMAS,
	ENABLE_ROW_LINEAGE
};

struct IcebergCommitState {
public:
	IcebergCommitState(const IcebergTableInformation &table_info, ClientContext &context);

public:
	const IcebergTableInformation &table_info;
	optional_ptr<const IcebergSnapshot> latest_snapshot;
	ClientContext &context;

	//! All the 'manifest_file' entries we will write to the new manifest list
	vector<IcebergManifestFile> manifests;
	rest_api_objects::CommitTableRequest table_change;
};

struct IcebergTableUpdate {
public:
	IcebergTableUpdate(IcebergTableUpdateType type, IcebergTableInformation &table_info);
	virtual ~IcebergTableUpdate() {
	}

public:
	virtual void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const = 0;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast IcebergTableUpdate to type - type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

public:
	IcebergTableUpdateType type;
	IcebergTableInformation &table_info;
};

} // namespace duckdb
