#pragma once

#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "rest_catalog/objects/list.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/snapshot/iceberg_row_id_allocator.hpp"

namespace duckdb {

struct IcebergTable;
struct IcebergTransactionData;
struct IcebergTableMetadata;
class IcebergSnapshotWriter;
struct IcebergWrittenSnapshot;
struct VersionedIcebergManifestDeletes;

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
private:
	//! Only transactions with format updates need a mutable serialization view.
	//! Other attempts use the refreshed catalog metadata directly, including retries.
	unique_ptr<IcebergTableMetadata> format_metadata;

public:
	IcebergCommitState(const IcebergTable &table_info, ClientContext &context);
	~IcebergCommitState();
	void LoadExistingManifests(DatabaseInstance &db, vector<IcebergManifestListEntry> &&existing_manifests);
	const IcebergTableMetadata &GetTableMetadata() const;
	void SetFormatVersion(int32_t format_version);
	optional_ptr<const IcebergSnapshot> GetLatestSnapshot() const;
	//! Start a snapshot and carry forward its existing manifests, applying this update's file invalidations.
	IcebergSnapshotWriter
	CreateSnapshotWriter(int32_t schema_id, IcebergSnapshotOperationType operation,
	                     optional_ptr<const VersionedIcebergManifestDeletes> manifest_deletes = nullptr);
	//! Adopt the written snapshot and its manifests together with the REST update that publishes it.
	void AddWrittenSnapshot(IcebergWrittenSnapshot written);

public:
	const IcebergTable &table_info;
	ClientContext &context;
	vector<string> created_metadata_files;

	rest_api_objects::CommitTableRequest table_change;

private:
	int64_t ReconstructTotalFilesSize(int32_t schema_id);
	void WriteExistingManifests(IcebergSnapshotWriter &writer, int32_t schema_id,
	                            optional_ptr<const VersionedIcebergManifestDeletes> manifest_deletes);

	//! Owned here between snapshots; writer creation transfers them into the next manifest list.
	vector<IcebergManifestListEntry> manifests;
	//! Earlier snapshots are retained in table_change; only the latest is needed as the next parent.
	optional<IcebergSnapshot> written_snapshot;
	sequence_number_t next_sequence_number;
	IcebergRowIdAllocator row_ids;
};

struct IcebergTableUpdate {
public:
	explicit IcebergTableUpdate(IcebergTableUpdateType type);
	virtual ~IcebergTableUpdate() {
	}

public:
	virtual void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const = 0;
	virtual bool IsRetryable() const {
		return false;
	}

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast IcebergTableUpdate to type - type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast IcebergTableUpdate to type - type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

public:
	IcebergTableUpdateType type;
};

} // namespace duckdb
