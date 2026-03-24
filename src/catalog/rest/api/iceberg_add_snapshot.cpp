#include "catalog/rest/api/iceberg_add_snapshot.hpp"

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/caching_file_system.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "catalog/rest/iceberg_table_set.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

namespace duckdb {

IcebergAddSnapshot::IcebergAddSnapshot(const IcebergTableInformation &table_info)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SNAPSHOT, table_info) {
}

static rest_api_objects::TableUpdate CreateAddSnapshotUpdate(const IcebergTableInformation &table_info,
                                                             const IcebergSnapshot &snapshot) {
	rest_api_objects::TableUpdate table_update;

	table_update.has_add_snapshot_update = true;
	auto &update = table_update.add_snapshot_update;
	update.base_update.action = "add-snapshot";
	update.has_action = true;
	update.action = "add-snapshot";
	update.snapshot = snapshot.ToRESTObject(table_info.table_metadata);
	return table_update;
}

IcebergManifestListEntry IcebergAddSnapshot::ConstructManifest(CopyFunction &avro_copy, DatabaseInstance &db,
                                                               IcebergCommitState &commit_state,
                                                               const IcebergManifestListEntry &list_entry,
                                                               const IcebergManifestDeletes &deletes) const {
	vector<IcebergManifestListEntry> manifest_files = {list_entry};
	IcebergOptions options;
	auto &fs = FileSystem::GetFileSystem(commit_state.context);
	auto &table_metadata = commit_state.table_info.table_metadata;
	auto &snapshot = *commit_state.latest_snapshot;
	auto manifest_scan =
	    AvroScan::ScanManifest(snapshot, manifest_files, options, fs, "", table_metadata, commit_state.context);
	auto manifest_file_reader = make_uniq<manifest_file::ManifestReader>(*manifest_scan);

	auto manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_file_path = fs.JoinPath(table_metadata.GetMetadataPath(fs), manifest_file_uuid + "-m0.avro");

	auto &rewritten_list_entry = manifest_files[0];
	auto &manifest_entries = rewritten_list_entry.manifest_entries;
	while (!manifest_file_reader->Finished()) {
		manifest_file_reader->Read();
	}
	auto &rewritten_manifest_file = rewritten_list_entry.file;
	rewritten_manifest_file.manifest_path = manifest_file_path;
	rewritten_manifest_file.added_files_count = 0;
	rewritten_manifest_file.deleted_files_count = 0;
	rewritten_manifest_file.existing_files_count = 0;
	rewritten_manifest_file.added_rows_count = 0;
	rewritten_manifest_file.deleted_rows_count = 0;
	rewritten_manifest_file.existing_rows_count = 0;

	bool removed_any_entries = false;
	idx_t handled_entries = 0;
	for (auto &manifest_entry : manifest_entries) {
		auto sequence_number = manifest_entry.GetSequenceNumber(rewritten_manifest_file);
		auto file_sequence_number = manifest_entry.GetFileSequenceNumber(rewritten_manifest_file);
		manifest_entry.SetSequenceNumber(sequence_number);
		manifest_entry.SetFileSequenceNumber(file_sequence_number);
		if (manifest_entry.status == IcebergManifestEntryStatusType::ADDED) {
			manifest_entry.status = IcebergManifestEntryStatusType::EXISTING;
		}
		//! File was already deleted, preserve the deleted entry
		if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
			rewritten_manifest_file.deleted_rows_count += manifest_entry.data_file.record_count;
			rewritten_manifest_file.deleted_files_count++;
			continue;
		}
		auto delete_it = deletes.altered_data_files.find(manifest_entry.data_file.file_path);
		if (delete_it == deletes.altered_data_files.end()) {
			rewritten_manifest_file.existing_rows_count += manifest_entry.data_file.record_count;
			rewritten_manifest_file.existing_files_count++;
			continue;
		}
		handled_entries++;
		auto &delete_file_state = delete_it->second;
		if (delete_file_state.type != IcebergDeleteType::DELETION_VECTOR) {
			//! This is a positional delete,
			//! we can't remove the file because we can't be sure
			//! that it doesn't contain deletes for other data files.
			rewritten_manifest_file.existing_rows_count += manifest_entry.data_file.record_count;
			rewritten_manifest_file.existing_files_count++;
			continue;
		}
		removed_any_entries = true;
		D_ASSERT(delete_file_state.referenced_data_files.size() == 1);
		D_ASSERT(manifest_entry.data_file.referenced_data_file == delete_file_state.referenced_data_files[0]);
		//! Remove the entry
		manifest_entry.status = IcebergManifestEntryStatusType::DELETED;
		rewritten_manifest_file.deleted_rows_count += manifest_entry.data_file.record_count;
		rewritten_manifest_file.deleted_files_count++;
	}
	if (handled_entries != deletes.altered_data_files.size()) {
		throw InternalException("(ConstructManifest) We expected to find %d invalidated entries, but only found %d",
		                        deletes.altered_data_files.size(), handled_entries);
	}
	if (!removed_any_entries) {
		return list_entry;
	}

	//! Finally overwrite the input 'manifest_file' with our edited copy
	auto manifest_length = manifest_file::WriteToFile(table_metadata, rewritten_manifest_file, manifest_entries,
	                                                  avro_copy, db, commit_state.context);
	rewritten_manifest_file.manifest_length = manifest_length;
	return std::move(rewritten_list_entry);
}

void IcebergAddSnapshot::ConstructManifestList(IcebergManifestList &new_manifest_list, CopyFunction &avro_copy,
                                               DatabaseInstance &db, IcebergCommitState &commit_state) const {
	//! Construct the manifest list
	if (altered_manifests.empty()) {
		//! Copy the existing manifest_file entries without any modification
		new_manifest_list.AddToManifestEntries(commit_state.manifests);
		return;
	}

	idx_t handled_entries = 0;
	for (auto &manifest_list_entry : commit_state.manifests) {
		auto &manifest_file = manifest_list_entry.file;
		auto it = altered_manifests.find(manifest_file.manifest_path);
		if (it == altered_manifests.end()) {
			new_manifest_list.AddExistingManifestFile(std::move(manifest_file));
			continue;
		}
		handled_entries++;
		auto &altered_manifest = it->second;
		auto new_manifest_file = ConstructManifest(avro_copy, db, commit_state, manifest_file, altered_manifest);
		new_manifest_list.AddNewManifestFile(std::move(new_manifest_file));
	}
	if (handled_entries != altered_manifests.size()) {
		throw InternalException("(ConstructManifestList) We expected to find %d invalidated entries, but only found %d",
		                        altered_manifests.size(), handled_entries);
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

void IcebergAddSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context,
                                      IcebergCommitState &commit_state) const {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	D_ASSERT(avro_copy_p);
	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;

	const auto &uncommitted_manifest_files = manifest_files;
	D_ASSERT(!uncommitted_manifest_files.empty());

	auto &table_metadata = commit_state.table_info.table_metadata;

	const auto snapshot_id = IcebergSnapshot::NewSnapshotId();
	const auto sequence_number = commit_state.next_sequence_number++;

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path = fs.JoinPath(table_metadata.GetMetadataPath(fs),
	                                      "snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro");

	//! Create a new manifest list, populate it with the content of the old manifest list (altered if necessary)
	IcebergManifestList new_manifest_list(snapshot_id, sequence_number, manifest_list_path);
	ConstructManifestList(new_manifest_list, avro_copy, db, commit_state);

	//! Construct the snapshot
	IcebergSnapshot new_snapshot;
	new_snapshot.operation = IcebergSnapshotOperationType::OVERWRITE;
	new_snapshot.snapshot_id = snapshot_id;
	new_snapshot.sequence_number = sequence_number;
	new_snapshot.schema_id = table_metadata.current_schema_id;
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

		auto new_manifest_list_entry = WriteManifestListEntry(table_info, manifest_list_entry, avro_copy, db, context);
		new_manifest_list.AddNewManifestFile(std::move(new_manifest_list_entry));

		if (table_metadata.iceberg_version >= 3) {
			commit_state.next_row_id += manifest_file.existing_rows_count + manifest_file.added_rows_count;

			if (manifest_file.content == IcebergManifestContentType::DATA) {
				new_snapshot.added_rows += manifest_file.added_rows_count;
			}
		}
	}

	manifest_list::WriteToFile(table_metadata, new_manifest_list, avro_copy, db, context);
	commit_state.manifests = new_manifest_list.GetManifestListEntries();

	commit_state.created_snapshots.push_back(new_snapshot);
	commit_state.latest_snapshot = commit_state.created_snapshots.back();

	//! Finally add a Iceberg REST Catalog 'TableUpdate' to commit
	commit_state.table_change.updates.push_back(CreateAddSnapshotUpdate(table_info, *commit_state.latest_snapshot));
}

void IcebergAddSnapshot::AddManifestFile(IcebergManifestListEntry &&manifest_file) {
	manifest_files.push_back(std::move(manifest_file));
}

const vector<IcebergManifestListEntry> &IcebergAddSnapshot::GetManifestFiles() const {
	return manifest_files;
}

} // namespace duckdb
