#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/optional.hpp"

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
	void SetManifestDeletes(VersionedIcebergManifestDeletes manifest_deletes);
	IcebergSnapshotOperationType GetOperation() const {
		return operation;
	}

private:
	vector<IcebergPendingManifest> pending_manifests;
	optional<VersionedIcebergManifestDeletes> manifest_deletes;
	int32_t schema_id;
	IcebergSnapshotOperationType operation;
};

} // namespace duckdb
