#include "catalog/rest/api/iceberg_add_snapshot.hpp"

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "catalog/rest/iceberg_table_set.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

namespace duckdb {

IcebergAddSnapshot::IcebergAddSnapshot(const IcebergTableMetadata &table_metadata,
                                       IcebergSnapshotOperationType operation)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SNAPSHOT), operation(operation) {
	schema_id = table_metadata.GetCurrentSchemaId();
}

bool IcebergAddSnapshot::IsRetryable() const {
	//! Only retry INSERT for now
	return operation == IcebergSnapshotOperationType::APPEND;
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

static IcebergManifestListEntry ScanExistingManifestFile(const IcebergManifestFile &manifest_file,
                                                         IcebergCommitState &commit_state, int32_t schema_id) {
	vector<IcebergManifestListEntry> manifest_files;
	manifest_files.push_back(manifest_file);

	IcebergOptions options;
	auto &fs = FileSystem::GetFileSystem(commit_state.context);
	auto &table_metadata = commit_state.table_info.table_metadata;

	IcebergSnapshotScanInfo snapshot_info;
	snapshot_info.snapshot = commit_state.latest_snapshot;
	snapshot_info.schema_id = schema_id;

	auto manifest_scan =
	    AvroScan::ScanManifest(snapshot_info, manifest_files, options, fs, "", table_metadata, commit_state.context);
	auto manifest_file_reader = make_uniq<manifest_file::ManifestReader>(*manifest_scan);

	while (!manifest_file_reader->Finished()) {
		manifest_file_reader->Read();
	}

	return std::move(manifest_files[0]);
}

static bool ManifestFileNeedsToBeRewritten(IcebergCommitState &commit_state, IcebergManifestListEntry &list_entry,
                                           const IcebergManifestDeletes &deletes) {
	auto &fs = FileSystem::GetFileSystem(commit_state.context);
	auto &table_metadata = commit_state.table_info.table_metadata;

	auto manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_file_path = fs.JoinPath(table_metadata.GetMetadataPath(fs), manifest_file_uuid + "-m0.avro");

	auto &manifest_entries = list_entry.manifest_entries;
	auto &manifest_file = list_entry.file;
	manifest_file.manifest_path = manifest_file_path;

	manifest_file.added_files_count = 0;
	manifest_file.deleted_files_count = 0;
	manifest_file.existing_files_count = 0;

	manifest_file.added_rows_count = 0;
	manifest_file.deleted_rows_count = 0;
	manifest_file.existing_rows_count = 0;

	bool removed_any_entries = false;
	for (auto &manifest_entry : manifest_entries) {
		auto sequence_number = manifest_entry.GetSequenceNumber(manifest_file);
		auto file_sequence_number = manifest_entry.GetFileSequenceNumber(manifest_file);
		manifest_entry.SetSequenceNumber(sequence_number);
		manifest_entry.SetFileSequenceNumber(file_sequence_number);
		if (manifest_entry.status == IcebergManifestEntryStatusType::ADDED) {
			manifest_entry.status = IcebergManifestEntryStatusType::EXISTING;
		}
		//! File was already deleted, preserve the deleted entry
		if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
			manifest_file.deleted_rows_count += manifest_entry.data_file.record_count;
			manifest_file.deleted_files_count++;
			continue;
		}
		auto is_deleted = deletes.IsInvalidated(manifest_entry.data_file.file_path);
		if (!is_deleted) {
			manifest_file.existing_rows_count += manifest_entry.data_file.record_count;
			manifest_file.existing_files_count++;
			continue;
		}
		removed_any_entries = true;
		//! Remove the entry
		manifest_entry.status = IcebergManifestEntryStatusType::DELETED;
		manifest_file.deleted_rows_count += manifest_entry.data_file.record_count;
		manifest_file.deleted_files_count++;
	}
	return removed_any_entries;
}

static void RewriteManifestFile(IcebergManifestListEntry &list_entry, CopyFunction &avro_copy, DatabaseInstance &db,
                                IcebergCommitState &commit_state) {
	auto &manifest_file = list_entry.file;
	auto &manifest_entries = list_entry.manifest_entries;
	auto &table_metadata = commit_state.table_info.table_metadata;

	//! Finally overwrite the input 'manifest_file' with our edited copy
	auto manifest_length = manifest_file::WriteToFile(table_metadata, manifest_file, manifest_entries, avro_copy, db,
	                                                  commit_state.context);
	manifest_file.manifest_length = manifest_length;
}

void IcebergAddSnapshot::ConstructManifestList(IcebergManifestList &new_manifest_list, CopyFunction &avro_copy,
                                               DatabaseInstance &db, IcebergCommitState &commit_state) const {
	//! Construct the manifest list
	//! FIXME: RETRY_BLOCKER: no guarantee that no new deletes are introduced
	if (altered_manifests.IsEmpty()) {
		//! Copy the existing manifest_file entries without any modification
		new_manifest_list.AddToManifestEntries(commit_state.manifests);
		return;
	}

	for (auto &manifest_list_entry : commit_state.manifests) {
		auto &existing_manifest_file = manifest_list_entry.file;

		auto scanned_manifest_file = ScanExistingManifestFile(existing_manifest_file, commit_state, schema_id);
		bool needs_rewrite = ManifestFileNeedsToBeRewritten(commit_state, scanned_manifest_file, altered_manifests);
		if (!needs_rewrite) {
			new_manifest_list.AddExistingManifestFile(std::move(manifest_list_entry));
			continue;
		}

		RewriteManifestFile(scanned_manifest_file, avro_copy, db, commit_state);
		new_manifest_list.AddNewManifestFile(std::move(scanned_manifest_file));
	}
	commit_state.manifests.clear();
}

static IcebergManifestListEntry WriteManifestListEntry(const IcebergTableInformation &table_info,
                                                       const IcebergManifestListEntry &list_entry,
                                                       CopyFunction &avro_copy, DatabaseInstance &db,
                                                       ClientContext &context) {
	auto manifest_length = manifest_file::WriteToFile(table_info.table_metadata, list_entry.file,
	                                                  list_entry.manifest_entries, avro_copy, db, context);
	IcebergManifestListEntry new_entry(list_entry.file);
	new_entry.manifest_entries = list_entry.manifest_entries;
	new_entry.file.manifest_length = manifest_length;
	return new_entry;
}

static vector<IcebergManifestListEntry>
CreateCommitManifestFiles(const vector<IcebergManifestListEntry> &manifest_files, IcebergCommitState &commit_state,
                          int64_t snapshot_id, int64_t sequence_number) {
	vector<IcebergManifestListEntry> result;
	result.reserve(manifest_files.size());
	auto &fs = FileSystem::GetFileSystem(commit_state.context);
	auto next_row_id = commit_state.next_row_id;
	for (const auto &manifest_entry : manifest_files) {
		auto copied_entries = manifest_entry.manifest_entries;
		auto copied_manifest = IcebergManifestListEntry::CreateFromEntries(
		    fs, snapshot_id, sequence_number, commit_state.table_info.table_metadata, manifest_entry.file.content,
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
	    CreateCommitManifestFiles(manifest_files, commit_state, snapshot_id, sequence_number);
	D_ASSERT(!uncommitted_manifest_files.empty());

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path = fs.JoinPath(table_metadata.GetMetadataPath(fs),
	                                      "snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro");

	//! Create a new manifest list, populate it with the content of the old manifest list (altered if necessary)
	IcebergManifestList new_manifest_list(snapshot_id, sequence_number, manifest_list_path);
	ConstructManifestList(new_manifest_list, avro_copy, db, commit_state);

	//! Construct the snapshot
	IcebergSnapshot new_snapshot;
	new_snapshot.operation = operation;
	new_snapshot.snapshot_id = snapshot_id;
	new_snapshot.sequence_number = sequence_number;
	new_snapshot.SetSchemaId(schema_id);
	new_snapshot.manifest_list = manifest_list_path;
	new_snapshot.timestamp_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());

	optional_ptr<const IcebergSnapshot> parent_snapshot = commit_state.latest_snapshot;
	if (parent_snapshot) {
		new_snapshot.has_parent_snapshot = true;
		new_snapshot.metrics = IcebergSnapshotMetrics(*parent_snapshot);
		new_snapshot.parent_snapshot_id = parent_snapshot->snapshot_id;
	}

	if (table_metadata.iceberg_version >= 3) {
		new_snapshot.has_first_row_id = true;
		new_snapshot.first_row_id = commit_state.next_row_id;
		new_snapshot.has_added_rows = true;
	}

	new_snapshot.added_rows = 0;
	for (auto &manifest_list_entry : uncommitted_manifest_files) {
		auto &manifest_file = manifest_list_entry.file;
		new_snapshot.metrics.AddManifestFile(manifest_file);

		auto new_manifest_list_entry =
		    WriteManifestListEntry(commit_state.table_info, manifest_list_entry, avro_copy, db, context);
		commit_state.created_metadata_files.push_back(new_manifest_list_entry.file.manifest_path);
		new_manifest_list.AddNewManifestFile(std::move(new_manifest_list_entry));

		if (table_metadata.iceberg_version >= 3) {
			commit_state.next_row_id += manifest_file.existing_rows_count + manifest_file.added_rows_count;

			if (manifest_file.content == IcebergManifestContentType::DATA) {
				new_snapshot.added_rows += manifest_file.added_rows_count;
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
