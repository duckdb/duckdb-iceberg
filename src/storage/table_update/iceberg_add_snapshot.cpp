#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "storage/catalog/iceberg_table_set.hpp"

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/caching_file_system.hpp"
#include "avro_scan.hpp"
#include "manifest_reader.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

IcebergAddSnapshot::IcebergAddSnapshot(IcebergTableInformation &table_info, const string &manifest_list_path,
                                       IcebergSnapshot &&snapshot)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SNAPSHOT, table_info), manifest_list(manifest_list_path),
      snapshot(std::move(snapshot)) {
}

static rest_api_objects::TableUpdate CreateAddSnapshotUpdate(const IcebergTableInformation &table_info,
                                                             const IcebergSnapshot &snapshot) {
	rest_api_objects::TableUpdate table_update;

	table_update.has_add_snapshot_update = true;
	auto &update = table_update.add_snapshot_update;
	update.base_update.action = "add-snapshot";
	update.has_action = true;
	update.action = "add-snapshot";
	update.snapshot = snapshot.ToRESTObject(table_info);
	return table_update;
}

void IcebergAddSnapshot::ConstructManifest(CopyFunction &avro_copy, DatabaseInstance &db,
                                           IcebergCommitState &commit_state, IcebergManifestFile &manifest_file,
                                           IcebergManifestDeletes &deletes) {
	vector<IcebergManifestFile> manifest_files = {manifest_file};
	IcebergOptions options;
	auto &fs = FileSystem::GetFileSystem(commit_state.context);
	auto &table_metadata = commit_state.table_info.table_metadata;
	auto &snapshot = *commit_state.latest_snapshot;
	auto manifest_scan =
	    AvroScan::ScanManifest(snapshot, manifest_files, options, fs, "", table_metadata, commit_state.context);
	auto manifest_file_reader = make_uniq<manifest_file::ManifestReader>(*manifest_scan, true);

	auto manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_file_path = table_metadata.GetMetadataPath() + "/" + manifest_file_uuid + "-m0.avro";
	IcebergManifest rewritten_manifest(manifest_file_path);
	while (!manifest_file_reader->Finished()) {
		manifest_file_reader->Read(STANDARD_VECTOR_SIZE, rewritten_manifest.entries);
	}
	auto &rewritten_manifest_file = manifest_files[0];
	rewritten_manifest_file.manifest_path = manifest_file_path;
	rewritten_manifest_file.added_files_count = 0;
	rewritten_manifest_file.deleted_files_count = 0;
	rewritten_manifest_file.existing_files_count = 0;
	rewritten_manifest_file.added_rows_count = 0;
	rewritten_manifest_file.deleted_rows_count = 0;
	rewritten_manifest_file.existing_rows_count = 0;

	bool removed_any_entries = false;
	idx_t handled_entries = 0;
	for (auto &manifest_entry : rewritten_manifest.entries) {
		manifest_entry.status = IcebergManifestEntryStatusType::EXISTING;
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
		return;
	}

	//! Finally overwrite the input 'manifest_file' with our edited copy
	manifest_file = rewritten_manifest_file;
	auto manifest_length =
	    manifest_file::WriteToFile(table_metadata, rewritten_manifest, avro_copy, db, commit_state.context);
	manifest_file.manifest_length = manifest_length;
}

void IcebergAddSnapshot::ConstructManifestList(CopyFunction &avro_copy, DatabaseInstance &db,
                                               IcebergCommitState &commit_state) {
	if (altered_manifests.empty()) {
		//! Copy the existing manifest_file entries without any modification
		manifest_list.AddToManifestEntries(commit_state.manifests);
		return;
	}

	idx_t handled_entries = 0;
	for (auto &manifest_file : commit_state.manifests) {
		auto it = altered_manifests.find(manifest_file.manifest_path);
		if (it == altered_manifests.end()) {
			manifest_list.manifest_entries.push_back(std::move(manifest_file));
			continue;
		}
		handled_entries++;
		auto &altered_manifest = it->second;
		ConstructManifest(avro_copy, db, commit_state, manifest_file, altered_manifest);
		manifest_list.manifest_entries.push_back(std::move(manifest_file));
	}
	if (handled_entries != altered_manifests.size()) {
		throw InternalException("(ConstructManifestList) We expected to find %d invalidated entries, but only found %d",
		                        altered_manifests.size(), handled_entries);
	}
	commit_state.manifests.clear();
}

void IcebergAddSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	D_ASSERT(avro_copy_p);
	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;
	D_ASSERT(manifest_list.GetManifestListEntriesCount() != 0);

	//! Write the avro files for the new manifests
	auto manifest_list_entries_size = manifest_list.GetManifestListEntriesCount();
	for (idx_t manifest_index = 0; manifest_index < manifest_list_entries_size; manifest_index++) {
		manifest_list.WriteManifestListEntry(table_info, manifest_index, avro_copy, db, context);
	}

	//! Add manifest_file entries from the previous snapshot (if any)
	ConstructManifestList(avro_copy, db, commit_state);
	manifest_list::WriteToFile(table_info.table_metadata, manifest_list, avro_copy, db, context);
	commit_state.manifests = manifest_list.GetManifestListEntries();
	commit_state.latest_snapshot = snapshot;

	//! Finally add a Iceberg REST Catalog 'TableUpdate' to commit
	commit_state.table_change.updates.push_back(CreateAddSnapshotUpdate(table_info, snapshot));
}

} // namespace duckdb
