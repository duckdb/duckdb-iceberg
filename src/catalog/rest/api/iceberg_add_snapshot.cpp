#include "catalog/rest/api/iceberg_add_snapshot.hpp"

#include "catalog/rest/api/iceberg_manifest_merge.hpp"

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "catalog/rest/iceberg_table_set.hpp"
#include "common/iceberg_utils.hpp"

namespace duckdb {

IcebergAddSnapshot::IcebergAddSnapshot(const IcebergTableInformation &table_info,
                                       IcebergSnapshotOperationType operation)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SNAPSHOT), operation(operation) {
	//! FIXME: Do we also need to capture the current partition spec and sort order?
	//! This is a bit of a code smell, the `IcebergTableInformation` should instead be const
	//! and all transactional changes should live in the IcebergTransactionData
	schema_id = table_info.table_metadata.GetCurrentSchemaId();
}

bool IcebergAddSnapshot::IsRetryable() const {
	//! DELETE-retry safety is enforced in StageSingleTableCommit.
	return operation == IcebergSnapshotOperationType::APPEND || operation == IcebergSnapshotOperationType::DELETE;
}

static rest_api_objects::TableUpdate CreateAddSnapshotUpdate(const IcebergTableInformation &table_info,
                                                             const IcebergSnapshot &snapshot) {
	rest_api_objects::TableUpdate table_update;

	table_update.add_snapshot_update = rest_api_objects::AddSnapshotUpdate();
	auto &update = *table_update.add_snapshot_update;
	update.base_update.action = "add-snapshot";
	update.snapshot = snapshot.ToRESTObject(table_info.table_metadata);
	return table_update;
}

static optional<IcebergManifestListEntry> RewriteManifestFile(const IcebergManifestListEntry &list_entry,
                                                              CopyFunction &avro_copy, DatabaseInstance &db,
                                                              IcebergCommitState &commit_state, int32_t schema_id,
                                                              const IcebergManifestDeletes &deletes,
                                                              IcebergSnapshotMetrics &snapshot_metrics) {
	auto loaded_manifest = list_entry.HasManifestEntries()
	                           ? list_entry
	                           : IcebergManifestMerge::ScanManifestEntries(list_entry, commit_state, schema_id);
	D_ASSERT(loaded_manifest.manifest_metadata);
	auto &scanned_entries = loaded_manifest.GetManifestEntries();

	vector<IcebergManifestEntry> rewritten_entries;
	rewritten_entries.reserve(scanned_entries.size());
	bool removed_any_entries = false;
	for (auto &manifest_entry : scanned_entries) {
		auto sequence_number = manifest_entry.GetSequenceNumber(loaded_manifest.file);
		auto file_sequence_number = manifest_entry.GetFileSequenceNumber(loaded_manifest.file);
		manifest_entry.SetSequenceNumber(sequence_number);
		manifest_entry.SetFileSequenceNumber(file_sequence_number);
		if (manifest_entry.status == IcebergManifestEntryStatusType::ADDED) {
			manifest_entry.status = IcebergManifestEntryStatusType::EXISTING;
		}
		if (manifest_entry.status != IcebergManifestEntryStatusType::DELETED &&
		    deletes.IsInvalidated(manifest_entry.data_file.file_path)) {
			snapshot_metrics.RemoveFileSize(manifest_entry.data_file.file_size_in_bytes);
			manifest_entry.status = IcebergManifestEntryStatusType::DELETED;
			removed_any_entries = true;
		}
		rewritten_entries.push_back(std::move(manifest_entry));
	}
	if (!removed_any_entries) {
		return nullopt;
	}
	return IcebergManifestMerge::WriteReplacementManifest(*loaded_manifest.manifest_metadata,
	                                                      std::move(rewritten_entries), avro_copy, db, commit_state,
	                                                      loaded_manifest.file.first_row_id);
}

static void AddManifestListEntry(IcebergManifestList &new_manifest_list, IcebergManifestListEntry &&manifest_entry) {
	if (!manifest_entry.file.added_snapshot_id) {
		new_manifest_list.AddNewManifestFile(std::move(manifest_entry));
		return;
	}
	new_manifest_list.AddExistingManifestFile(std::move(manifest_entry));
}

void IcebergAddSnapshot::ConstructManifestList(IcebergManifestList &new_manifest_list, CopyFunction &avro_copy,
                                               DatabaseInstance &db, IcebergCommitState &commit_state,
                                               IcebergSnapshotMetrics &snapshot_metrics) const {
	//! Construct the manifest list
	//! FIXME: RETRY_BLOCKER: no guarantee that no new deletes are introduced
	if (altered_manifests.IsEmpty()) {
		for (auto &manifest_list_entry : commit_state.manifests) {
			AddManifestListEntry(new_manifest_list, std::move(manifest_list_entry));
		}
		commit_state.manifests.clear();
		return;
	}

	for (auto &manifest_list_entry : commit_state.manifests) {
		auto rewritten_manifest = RewriteManifestFile(manifest_list_entry, avro_copy, db, commit_state, schema_id,
		                                              altered_manifests, snapshot_metrics);
		if (!rewritten_manifest) {
			AddManifestListEntry(new_manifest_list, std::move(manifest_list_entry));
			continue;
		}

		new_manifest_list.AddNewManifestFile(std::move(*rewritten_manifest));
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
			total_files_size = IcebergUtils::AddFileSizeChecked(total_files_size, entry.data_file.file_size_in_bytes);
		}
	}
	return total_files_size;
}

static IcebergManifestListEntry WriteManifestListEntry(const IcebergTableInformation &table_info,
                                                       const IcebergManifestListEntry &list_entry,
                                                       CopyFunction &avro_copy, DatabaseInstance &db,
                                                       ClientContext &context) {
	D_ASSERT(list_entry.manifest_metadata);
	IcebergManifestListEntry new_entry(list_entry.file, *list_entry.manifest_metadata);
	new_entry.manifest_entries = list_entry.manifest_entries;
	auto manifest_length = manifest_file::WriteToFile(table_info.table_metadata, new_entry, avro_copy, db, context);
	new_entry.file.manifest_length = manifest_length;
	return new_entry;
}

static vector<IcebergManifestListEntry>
CreateCommitManifestFiles(const vector<IcebergManifestListEntry> &manifest_files,
                          const IcebergTableInformation &table_info, IcebergCommitState &commit_state,
                          int64_t sequence_number) {
	vector<IcebergManifestListEntry> result;
	result.reserve(manifest_files.size());
	auto &fs = FileSystem::GetFileSystem(commit_state.context);
	auto next_row_id = commit_state.next_row_id;
	for (const auto &manifest_entry : manifest_files) {
		D_ASSERT(manifest_entry.manifest_metadata);
		auto copied_entries = manifest_entry.GetManifestEntries();
		auto copied_manifest = IcebergManifestListEntry::CreateFromEntries(
		    fs, sequence_number, table_info.table_metadata, *manifest_entry.manifest_metadata,
		    std::move(copied_entries), next_row_id);
		result.push_back(std::move(copied_manifest));
	}
	return result;
}

void IcebergAddSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                      IcebergCommitState &commit_state) const {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	D_ASSERT(avro_copy_p);
	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;

	auto &table_metadata = commit_state.table_info.table_metadata;
	const auto snapshot_id = IcebergSnapshot::NewSnapshotId();
	const auto sequence_number = commit_state.next_sequence_number++;
	auto uncommitted_manifest_files =
	    CreateCommitManifestFiles(manifest_files, commit_state.table_info, commit_state, sequence_number);
	D_ASSERT(!uncommitted_manifest_files.empty());

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path = fs.JoinPath(table_metadata.GetMetadataPath(fs),
	                                      "snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro");

	//! Construct the snapshot
	IcebergSnapshot new_snapshot(schema_id);
	new_snapshot.operation = operation;
	new_snapshot.snapshot_id = snapshot_id;
	new_snapshot.sequence_number = sequence_number;
	new_snapshot.manifest_list = manifest_list_path;
	new_snapshot.timestamp_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());

	optional_ptr<const IcebergSnapshot> parent_snapshot = commit_state.latest_snapshot;
	if (parent_snapshot) {
		new_snapshot.metrics = IcebergSnapshotMetrics(*parent_snapshot);
		new_snapshot.parent_snapshot_id = parent_snapshot->snapshot_id;
		if (!new_snapshot.metrics.HasTotalFilesSize()) {
			new_snapshot.metrics.SetTotalFilesSize(ReconstructTotalFilesSize(commit_state, schema_id));
		}
	}

	//! Create a new manifest list, populate it with the content of the old manifest list (altered if necessary)
	IcebergManifestList new_manifest_list(snapshot_id, sequence_number, manifest_list_path);
	ConstructManifestList(new_manifest_list, avro_copy, db, commit_state, new_snapshot.metrics);

	if (table_metadata.iceberg_version >= 3) {
		new_snapshot.first_row_id = commit_state.next_row_id;
		new_snapshot.added_rows = 0;
	}

	for (auto &manifest_list_entry : uncommitted_manifest_files) {
		auto &manifest_file = manifest_list_entry.file;
		new_snapshot.metrics.AddManifestListEntry(manifest_list_entry);

		auto new_manifest_list_entry =
		    WriteManifestListEntry(commit_state.table_info, manifest_list_entry, avro_copy, db, context);
		commit_state.created_metadata_files.push_back(new_manifest_list_entry.file.manifest_path);
		new_manifest_list.AddNewManifestFile(std::move(new_manifest_list_entry));

		if (table_metadata.iceberg_version >= 3) {
			commit_state.next_row_id += manifest_file.existing_rows_count + manifest_file.added_rows_count;

			if (manifest_file.content == IcebergManifestContentType::DATA) {
				*new_snapshot.added_rows += manifest_file.added_rows_count;
			}
		}
	}

	manifest_list::WriteToFile(table_metadata, new_manifest_list, avro_copy, db, context);
	commit_state.created_metadata_files.push_back(manifest_list_path);
	commit_state.manifests = new_manifest_list.GetManifestListEntries();

	commit_state.created_snapshots.push_back(new_snapshot);
	commit_state.latest_snapshot = commit_state.created_snapshots.back();

	//! Finally add a Iceberg REST Catalog 'TableUpdate' to commit
	commit_state.table_change.updates.push_back(
	    CreateAddSnapshotUpdate(commit_state.table_info, *commit_state.latest_snapshot));
}

void IcebergAddSnapshot::AddManifestFile(IcebergManifestListEntry &&manifest_file) {
	manifest_files.push_back(std::move(manifest_file));
}

const vector<IcebergManifestListEntry> &IcebergAddSnapshot::GetManifestFiles() const {
	return manifest_files;
}

} // namespace duckdb
