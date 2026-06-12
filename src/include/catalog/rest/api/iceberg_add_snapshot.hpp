#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

#include "catalog/rest/api/iceberg_table_update.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/transaction/iceberg_transaction_metadata.hpp"

namespace duckdb {

struct IcebergTableInformation;
struct IcebergManifestList;

struct IcebergAddSnapshot : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SNAPSHOT;

public:
	IcebergAddSnapshot(const IcebergTableInformation &table_info, IcebergSnapshotOperationType operation);

public:
	void ConstructManifestList(IcebergManifestList &manifest_list, CopyFunction &avro_copy, DatabaseInstance &db,
	                           IcebergCommitState &commit_state) const;
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;
	//! Pure, repeatable core of CreateUpdate: builds the new manifest list + snapshot from the current
	//! state in `commit_state` (parent snapshot, next sequence number, next row id) and writes them out.
	//! Safe to call once per commit attempt during a retry loop, provided `commit_state` reflects the
	//! freshly-refreshed metadata baseline (a retry reconstructs IcebergCommitState from it).
	void ApplyTableChanges(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const;
	const vector<IcebergManifestListEntry> &GetManifestFiles() const;
	void AddManifestFile(IcebergManifestListEntry &&manifest_file);
	//! Reassign the V3 manifest-file-level first_row_id of this snapshot's pending DATA manifests,
	//! advancing `next_row_id`. Used on retry: the row-id baseline was materialized when the snapshot
	//! was first staged, but a concurrent commit may have advanced the table's next_row_id, so the
	//! pending manifests must be re-based onto the refreshed baseline to keep row lineage contiguous.
	//! No-op for V2 (no row lineage). DELETE manifests never carry a first_row_id.
	void ReassignFirstRowIds(int64_t &next_row_id);
	//! The snapshot id this update will commit. Generated once and kept stable across retry attempts
	//! (Java memoizes the same way), so a status-check after an ambiguous commit can look for exactly
	//! this id in the refreshed table to decide whether the commit landed.
	int64_t GetSnapshotId() const;

private:
	//! MergeAppend (#790): repack the assembled manifest list into fewer manifests, in place.
	void MergeManifestList(IcebergManifestList &new_manifest_list, int64_t snapshot_id, CopyFunction &avro_copy,
	                       DatabaseInstance &db, IcebergCommitState &commit_state) const;

public:
	IcebergManifestDeletes altered_manifests;

private:
	vector<IcebergManifestListEntry> manifest_files;
	//! The logical operation this snapshot represents (APPEND/DELETE/OVERWRITE), declared by the
	//! caller that staged the change. Written into the committed snapshot summary's "operation".
	IcebergSnapshotOperationType operation;
	int32_t schema_id;
	//! Lazily assigned on first ApplyTableChanges and reused on every retry attempt. -1 = unassigned.
	mutable int64_t snapshot_id = -1;
};

} // namespace duckdb
