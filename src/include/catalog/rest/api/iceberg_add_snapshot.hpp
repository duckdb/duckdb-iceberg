#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/optional.hpp"

#include "catalog/rest/api/iceberg_table_update.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/transaction/iceberg_transaction_metadata.hpp"

namespace duckdb {

struct IcebergTable;
struct IcebergManifestList;

struct IcebergAddSnapshot : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SNAPSHOT;

public:
	IcebergAddSnapshot(const IcebergTable &table_info,
	                   IcebergSnapshotOperationType operation = IcebergSnapshotOperationType::OVERWRITE);

public:
	bool IsRetryable() const override;
	void ConstructManifestList(IcebergManifestList &manifest_list, CopyFunction &avro_copy, DatabaseInstance &db,
	                           IcebergCommitState &commit_state, IcebergSnapshotMetrics &snapshot_metrics) const;
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;
	const vector<IcebergManifestListEntry> &GetManifestFiles() const;
	void AddManifestFile(IcebergManifestListEntry &&manifest_file);
	void SetManifestDeletes(VersionedIcebergManifestDeletes manifest_deletes);
	IcebergSnapshotOperationType GetOperation() const {
		return operation;
	}

private:
	vector<IcebergManifestListEntry> manifest_files;
	optional<VersionedIcebergManifestDeletes> manifest_deletes;
	int32_t schema_id;
	IcebergSnapshotOperationType operation;
};

} // namespace duckdb
