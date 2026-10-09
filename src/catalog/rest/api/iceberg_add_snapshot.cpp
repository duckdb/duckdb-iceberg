#include "catalog/rest/api/iceberg_add_snapshot.hpp"

#include "catalog/rest/api/iceberg_manifest_merge.hpp"

#include "core/metadata/snapshot/iceberg_snapshot_writer.hpp"
#include "catalog/rest/iceberg_table_set.hpp"
#include "common/iceberg_utils.hpp"

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

static rest_api_objects::TableUpdate CreateAddSnapshotUpdate(const IcebergTable &table_info,
                                                             const IcebergSnapshot &snapshot) {
	rest_api_objects::TableUpdate table_update;

	table_update.add_snapshot_update = rest_api_objects::AddSnapshotUpdate();
	auto &update = *table_update.add_snapshot_update;
	update.base_update.action = "add-snapshot";
	update.snapshot = snapshot.ToRESTObject(table_info.table_metadata);
	return table_update;
}

static optional<IcebergManifestListEntry> RewriteManifestFile(const IcebergManifestListEntry &list_entry,
                                                              IcebergSnapshotWriter &writer,
                                                              IcebergCommitState &commit_state, int32_t schema_id,
                                                              const VersionedIcebergManifestDeletes &deletes) {
	auto loaded_manifest = list_entry.HasManifestEntries()
	                           ? list_entry
	                           : IcebergManifestMerge::ScanManifestEntries(list_entry, commit_state, schema_id);
	D_ASSERT(loaded_manifest.manifest_metadata);
	auto &scanned_entries = loaded_manifest.GetManifestEntries();
	const auto &file = loaded_manifest.GetFile();

	vector<IcebergManifestEntry> rewritten_entries;
	rewritten_entries.reserve(scanned_entries.size());
	bool removed_any_entries = false;
	for (auto &manifest_entry : scanned_entries) {
		auto sequence_number = manifest_entry.GetSequenceNumber(file);
		auto file_sequence_number = manifest_entry.GetFileSequenceNumber(file);
		manifest_entry.SetSequenceNumber(sequence_number);
		manifest_entry.SetFileSequenceNumber(file_sequence_number);
		//! The replacement manifest is added by this snapshot, so carried-over entries must keep their snapshot id
		manifest_entry.SetSnapshotId(manifest_entry.GetSnapshotId(file));
		if (manifest_entry.status == IcebergManifestEntryStatusType::ADDED) {
			manifest_entry.status = IcebergManifestEntryStatusType::EXISTING;
		}
		if (manifest_entry.status != IcebergManifestEntryStatusType::DELETED &&
		    deletes.IsInvalidated({manifest_entry.data_file.file_path, manifest_entry.data_file.content_offset})) {
			writer.RemoveManifestEntry(manifest_entry);
			manifest_entry.status = IcebergManifestEntryStatusType::DELETED;
			//! Inherits this snapshot, which deleted the file; conflict checks in other engines rely on it
			manifest_entry.SetSnapshotId(nullopt);
			removed_any_entries = true;
		}
		rewritten_entries.push_back(std::move(manifest_entry));
	}
	if (!removed_any_entries) {
		return nullopt;
	}
	return writer.WriteReplacementManifest(*loaded_manifest.manifest_metadata, std::move(rewritten_entries),
	                                       file.first_row_id);
}

void IcebergAddSnapshot::ConstructManifestList(IcebergSnapshotWriter &writer, IcebergCommitState &commit_state) const {
	for (auto &manifest : commit_state.manifests) {
		if (manifest_deletes) {
			auto replacement = RewriteManifestFile(manifest, writer, commit_state, schema_id, *manifest_deletes);
			if (replacement) {
				writer.AddExistingManifest(std::move(*replacement));
				continue;
			}
		}
		writer.AddExistingManifest(std::move(manifest));
	}
	commit_state.manifests.clear();
}

static int64_t ReconstructTotalFilesSize(IcebergCommitState &commit_state, int32_t schema_id) {
	int64_t total_files_size = 0;
	for (const auto &manifest : commit_state.manifests) {
		auto loaded_manifest = manifest.HasManifestEntries()
		                           ? manifest
		                           : IcebergManifestMerge::ScanManifestEntries(manifest, commit_state, schema_id);
		for (const auto &entry : loaded_manifest.GetManifestEntries()) {
			if (entry.status == IcebergManifestEntryStatusType::DELETED) {
				continue;
			}
			total_files_size =
			    IcebergUtils::AddFileSizeChecked(total_files_size, entry.data_file.GetContentSizeInBytes());
		}
	}
	return total_files_size;
}

void IcebergAddSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                      IcebergCommitState &commit_state) const {
	auto &table_metadata = commit_state.table_info.table_metadata;
	IcebergSnapshotWriter writer(context, table_metadata, schema_id, operation, commit_state.next_sequence_number++,
	                             commit_state.next_row_id, commit_state.created_metadata_files,
	                             commit_state.latest_snapshot);
	// Repack the base manifests once per commit attempt, with this snapshot's identity.
	if (commit_state.created_snapshots.empty()) {
		IcebergManifestMerge::MergeManifestList(commit_state.manifests, table_metadata.GetCurrentSchemaId(), writer,
		                                        commit_state);
	}
	if (commit_state.latest_snapshot && !commit_state.latest_snapshot->metrics.HasTotalFilesSize()) {
		writer.SetTotalFilesSize(ReconstructTotalFilesSize(commit_state, schema_id));
	}
	ConstructManifestList(writer, commit_state);
	for (const auto &manifest : pending_manifests) {
		writer.WriteManifest(manifest);
	}
	auto written = IcebergWrittenSnapshot::Create(std::move(writer));
	commit_state.next_row_id = written.next_row_id;
	commit_state.manifests = std::move(written.manifests);
	commit_state.created_snapshots.push_back(std::move(written.snapshot));
	commit_state.latest_snapshot = commit_state.created_snapshots.back();

	commit_state.table_change.updates.push_back(
	    CreateAddSnapshotUpdate(commit_state.table_info, *commit_state.latest_snapshot));
}

void IcebergAddSnapshot::AddPendingManifest(IcebergPendingManifest manifest) {
	pending_manifests.push_back(std::move(manifest));
}

void IcebergAddSnapshot::SetManifestDeletes(VersionedIcebergManifestDeletes manifest_deletes_p) {
	manifest_deletes.emplace(std::move(manifest_deletes_p));
}

const vector<IcebergPendingManifest> &IcebergAddSnapshot::GetPendingManifests() const {
	return pending_manifests;
}

} // namespace duckdb
