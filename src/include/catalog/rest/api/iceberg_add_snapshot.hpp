#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include "catalog/rest/api/iceberg_table_update.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/manifest/iceberg_pending_manifest.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/transaction/iceberg_transaction_metadata.hpp"

namespace duckdb {

struct IcebergTable;

struct IcebergAddSnapshot : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SNAPSHOT;

public:
	IcebergAddSnapshot(const IcebergTable &table_info,
	                   IcebergSnapshotOperationType operation = IcebergSnapshotOperationType::OVERWRITE);

public:
	bool IsRetryable() const override;
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;
	const vector<IcebergPendingManifest> &GetPendingManifests() const;
	void AddPendingManifest(IcebergPendingManifest manifest);
	//! Moves another MERGE action's manifests here; the caller merges its file invalidations separately.
	void Merge(IcebergAddSnapshot &other);
	void SetManifestDeletes(VersionedIcebergManifestDeletes manifest_deletes);
	//! A property of the snapshot's summary besides the operation and the metrics
	void SetSummaryProperty(const string &key, const string &value);
	IcebergSnapshotOperationType GetOperation() const {
		return operation;
	}

private:
	vector<IcebergPendingManifest> pending_manifests;
	optional<VersionedIcebergManifestDeletes> manifest_deletes;
	case_insensitive_map_t<string> summary_properties;
	int32_t schema_id;
	IcebergSnapshotOperationType operation;
};

} // namespace duckdb
