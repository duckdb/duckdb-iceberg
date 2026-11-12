#include "catalog/rest/api/iceberg_add_snapshot.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "core/metadata/snapshot/iceberg_snapshot_writer.hpp"

namespace duckdb {

IcebergAddSnapshot::IcebergAddSnapshot(const IcebergTable &table_info, IcebergSnapshotOperationType operation)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SNAPSHOT), operation(operation) {
	//! FIXME: Do we also need to capture the current partition spec and sort order?
	//! This is a bit of a code smell, the `IcebergTable` should instead be const
	//! and all transactional changes should live in the IcebergTransactionData
	schema_id = table_info.table_metadata.GetCurrentSchemaId();
}

bool IcebergAddSnapshot::IsRetryable() const {
	//! DELETE-retry safety is enforced in StageSingleTableCommit.
	return operation == IcebergSnapshotOperationType::APPEND || operation == IcebergSnapshotOperationType::DELETE;
}

void IcebergAddSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                      IcebergCommitState &commit_state) const {
	auto writer =
	    commit_state.CreateSnapshotWriter(schema_id, operation, manifest_deletes ? &*manifest_deletes : nullptr);
	for (const auto &manifest : pending_manifests) {
		writer.WriteManifest(manifest);
	}
	commit_state.AddWrittenSnapshot(IcebergWrittenSnapshot::Create(std::move(writer)));
}

void IcebergAddSnapshot::AddPendingManifest(IcebergPendingManifest manifest) {
	pending_manifests.push_back(std::move(manifest));
}

void IcebergAddSnapshot::Merge(IcebergAddSnapshot &other) {
	if (schema_id != other.schema_id) {
		throw InternalException("Cannot combine MERGE actions with different schemas");
	}
	// REPLACE describes maintenance, not a row-changing MERGE action.
	D_ASSERT(operation == IcebergSnapshotOperationType::APPEND || operation == IcebergSnapshotOperationType::DELETE ||
	         operation == IcebergSnapshotOperationType::OVERWRITE);
	D_ASSERT(other.operation == IcebergSnapshotOperationType::APPEND ||
	         other.operation == IcebergSnapshotOperationType::DELETE ||
	         other.operation == IcebergSnapshotOperationType::OVERWRITE);
	// AddSnapshotUpdate attaches file invalidations after assigning the shared alter index.
	// The incoming action must not already have versioned invalidations; it may still contain delete manifests.
	D_ASSERT(!other.manifest_deletes);
	// Mixed actions use overwrite semantics, including the conservative overwrite retry policy.
	if (operation != other.operation) {
		operation = IcebergSnapshotOperationType::OVERWRITE;
	}
	for (auto &manifest : other.pending_manifests) {
		AddPendingManifest(std::move(manifest));
	}
}

void IcebergAddSnapshot::SetManifestDeletes(VersionedIcebergManifestDeletes manifest_deletes_p) {
	manifest_deletes.emplace(std::move(manifest_deletes_p));
}

const vector<IcebergPendingManifest> &IcebergAddSnapshot::GetPendingManifests() const {
	return pending_manifests;
}

} // namespace duckdb
