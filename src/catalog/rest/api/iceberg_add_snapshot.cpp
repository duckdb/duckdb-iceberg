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
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

namespace duckdb {

IcebergAddSnapshot::IcebergAddSnapshot(const IcebergTableInformation &table_info,
                                       IcebergSnapshotOperationType operation)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SNAPSHOT, table_info), operation(operation) {
	//! FIXME: Do we also need to capture the current partition spec and sort order?
	//! This is a bit of a code smell, the `IcebergTableInformation` should instead be const
	//! and all transactional changes should live in the IcebergTransactionData
	schema_id = table_info.table_metadata.GetCurrentSchemaId();
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

void IcebergAddSnapshot::MergeManifestList(IcebergManifestList &new_manifest_list, int64_t snapshot_id,
                                           CopyFunction &avro_copy, DatabaseInstance &db,
                                           IcebergCommitState &commit_state) const {
	auto config = ManifestMergeConfig::FromTableMetadata(commit_state.table_info.table_metadata);
	if (!config.enabled) {
		return;
	}

	//! Merging manifests written under different schema ids is unsafe: the schema id determines the
	//! data_file.partition layout and column stats, so mixing them can drop or misalign stats. A
	//! manifest_file does not currently expose its schema id (that needs #1129), so until then we
	//! only merge when the table has a single schema -- then every manifest is unambiguously that
	//! schema. Tables that have gone through schema evolution skip merging for now.
	if (commit_state.table_info.table_metadata.GetSchemas().size() > 1) {
		return;
	}

	auto &entries = new_manifest_list.GetManifestFilesMutable();
	if (entries.size() <= 1) {
		return;
	}

	//! V3 row lineage: a row's first_row_id is a stable per-row identifier and, per spec, MUST be
	//! preserved when a manifest is rewritten/merged. Already-committed ("carried-over") manifests
	//! have their first_row_id settled, so merging them is safe (each merged manifest keeps the
	//! minimum first_row_id of the manifests it absorbs). This transaction's brand-new data
	//! manifests do not yet have per-entry first_row_id materialized -- those rows rely on
	//! manifest-level inheritance assigned later in manifest_list::WriteToFile -- so folding them
	//! into a merged manifest would break that inheritance basis. We therefore leave new V3 data
	//! manifests unmerged; next commit they are carried-over and become eligible, so growth still
	//! converges, merely delayed by one commit. V2 has no row lineage, so everything is eligible.
	const bool is_v3 = commit_state.table_info.table_metadata.iceberg_version >= 3;

	//! Split by content type, tagging each manifest's origin. Excluded manifests are kept aside.
	vector<MergeInputManifest> data_input;
	vector<MergeInputManifest> delete_input;
	vector<IcebergManifestListEntry> kept;
	for (auto &entry : entries) {
		auto source = entry.file.added_snapshot_id == snapshot_id ? ManifestSource::NEW_THIS_TRANSACTION
		                                                          : ManifestSource::CARRIED_OVER;
		//! V3 row lineage only constrains DATA manifests (first_row_id). New DELETE manifests carry
		//! no first_row_id, so they remain eligible for merging even on V3.
		if (is_v3 && source == ManifestSource::NEW_THIS_TRANSACTION &&
		    entry.file.content == IcebergManifestContentType::DATA) {
			kept.push_back(std::move(entry));
			continue;
		}
		auto &target = entry.file.content == IcebergManifestContentType::DELETE ? delete_input : data_input;
		target.push_back(MergeInputManifest {std::move(entry), source});
	}

	auto merged_data = MergeManifests(std::move(data_input), IcebergManifestContentType::DATA, config, avro_copy, db,
	                                  commit_state, schema_id, snapshot_id);
	auto merged_delete = MergeManifests(std::move(delete_input), IcebergManifestContentType::DELETE, config, avro_copy,
	                                    db, commit_state, schema_id, snapshot_id);

	//! Reassemble. A manifest produced by the merge is a brand-new file created by this snapshot, so
	//! stamp it with the snapshot's sequence number / id while preserving its computed
	//! min_sequence_number. The merge writes its products with a placeholder snapshot id of -1,
	//! which distinguishes them from new and carried-over manifests that pass through unmerged.
	entries.clear();
	auto reassemble = [&](vector<IcebergManifestListEntry> &merged) {
		for (auto &entry : merged) {
			if (entry.file.added_snapshot_id < 0) {
				entry.file.added_snapshot_id = snapshot_id;
				entry.file.sequence_number = new_manifest_list.GetSequenceNumber();
			}
			entries.push_back(std::move(entry));
		}
	};
	reassemble(merged_data);
	reassemble(merged_delete);
	//! Re-append manifests that were excluded from merging (e.g. V3 new-data manifests) unchanged.
	for (auto &entry : kept) {
		entries.push_back(std::move(entry));
	}
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
CreateCommitManifestFiles(const vector<IcebergManifestListEntry> &manifest_files,
                          const IcebergTableInformation &table_info, IcebergCommitState &commit_state,
                          int64_t snapshot_id, int64_t sequence_number) {
	vector<IcebergManifestListEntry> result;
	result.reserve(manifest_files.size());
	auto &fs = FileSystem::GetFileSystem(commit_state.context);
	auto next_row_id = commit_state.next_row_id;
	for (const auto &manifest_entry : manifest_files) {
		auto copied_entries = manifest_entry.manifest_entries;
		auto copied_manifest = IcebergManifestListEntry::CreateFromEntries(
		    fs, snapshot_id, sequence_number, table_info.table_metadata, manifest_entry.file.content,
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
	    CreateCommitManifestFiles(manifest_files, table_info, commit_state, snapshot_id, sequence_number);
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

		auto new_manifest_list_entry = WriteManifestListEntry(table_info, manifest_list_entry, avro_copy, db, context);
		commit_state.created_metadata_files.push_back(new_manifest_list_entry.file.manifest_path);
		new_manifest_list.AddNewManifestFile(std::move(new_manifest_list_entry));

		if (table_metadata.iceberg_version >= 3) {
			commit_state.next_row_id += manifest_file.existing_rows_count + manifest_file.added_rows_count;

			if (manifest_file.content == IcebergManifestContentType::DATA) {
				new_snapshot.added_rows += manifest_file.added_rows_count;
			}
		}
	}

	//! MergeAppend (#790): repack the assembled manifest list into fewer manifests before writing.
	MergeManifestList(new_manifest_list, snapshot_id, avro_copy, db, commit_state);

	manifest_list::WriteToFile(table_metadata, new_manifest_list, avro_copy, db, context);
	commit_state.created_metadata_files.push_back(manifest_list_path);
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
