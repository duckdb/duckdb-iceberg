#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/api/iceberg_manifest_merge.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "catalog/rest/transaction/iceberg_transaction_metadata.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/snapshot/iceberg_snapshot_writer.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"

namespace duckdb {

static void AssignManifestFirstRowIds(const IcebergTableMetadata &metadata,
                                      optional_ptr<const IcebergSnapshot> current_snapshot,
                                      vector<IcebergManifestListEntry> &existing_manifest_list,
                                      IcebergRowIdAllocator &row_ids) {
	if (metadata.iceberg_version < 3) {
		return;
	}
	for (auto &manifest_list_entry : existing_manifest_list) {
		auto &manifest_file = manifest_list_entry.GetManifest();
		if (manifest_file.content != IcebergManifestContentType::DATA) {
			continue;
		}
		if (!manifest_file.first_row_id && current_snapshot && current_snapshot->first_row_id) {
			throw InvalidConfigurationException(
			    "Table is corrupted, snapshot has 'first-row-id' but not all 'manifest_file' "
			    "entries have a 'first_row_id'");
		}
		row_ids.AssignExistingManifest(manifest_file);
	}
}

static unique_ptr<IcebergTableMetadata> CreateFormatMetadata(const IcebergTable &table_info) {
	if (table_info.transaction_data) {
		auto &transaction_data = *table_info.transaction_data;
		for (auto &update : transaction_data.updates) {
			if (update->type != IcebergTableUpdateType::UPGRADE_FORMAT_VERSION) {
				continue;
			}
			// Replay local format changes where they occurred, rather than serializing
			// earlier snapshots with the final version already stored on table_info.
			auto format_metadata = make_uniq<IcebergTableMetadata>(table_info.table_metadata.Copy());
			format_metadata->iceberg_version = transaction_data.initial_format_version;
			if (format_metadata->iceberg_version < 3) {
				format_metadata->next_row_id.reset();
			}
			return format_metadata;
		}
	}
	return nullptr;
}

IcebergCommitState::IcebergCommitState(const IcebergTable &table_info, ClientContext &context)
    : format_metadata(CreateFormatMetadata(table_info)), table_info(table_info), context(context),
      next_sequence_number(GetTableMetadata().last_sequence_number + 1),
      row_ids(GetTableMetadata().iceberg_version >= 3 ? GetTableMetadata().next_row_id.value_or(0) : 0) {
}

IcebergCommitState::~IcebergCommitState() = default;

const IcebergTableMetadata &IcebergCommitState::GetTableMetadata() const {
	return format_metadata ? *format_metadata : table_info.table_metadata;
}

optional_ptr<const IcebergSnapshot> IcebergCommitState::GetLatestSnapshot() const {
	if (written_snapshot) {
		return *written_snapshot;
	}
	return GetTableMetadata().GetLatestSnapshot();
}

int64_t IcebergCommitState::ReconstructTotalFilesSize(int32_t schema_id) {
	int64_t total_files_size = 0;
	for (const auto &manifest : manifests) {
		auto loaded_manifest = manifest.HasManifestEntries()
		                           ? manifest
		                           : IcebergManifestMerge::ScanManifestEntries(manifest, *this, schema_id);
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

static optional<IcebergManifestListEntry> RewriteManifestFile(const IcebergManifestListEntry &list_entry,
                                                              IcebergSnapshotWriter &writer,
                                                              IcebergCommitState &commit_state, int32_t schema_id,
                                                              const VersionedIcebergManifestDeletes &deletes) {
	auto loaded_manifest = list_entry.HasManifestEntries()
	                           ? list_entry
	                           : IcebergManifestMerge::ScanManifestEntries(list_entry, commit_state, schema_id);
	D_ASSERT(loaded_manifest.manifest_metadata);
	const auto &file = loaded_manifest.GetFile();

	auto rewritten_entries = file.PrepareEntriesForRewrite(std::move(loaded_manifest.GetManifestEntries()));
	bool removed_any_entries = false;
	for (auto &manifest_entry : rewritten_entries) {
		if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
			continue;
		}
		if (!deletes.IsInvalidated({manifest_entry.data_file.file_path, manifest_entry.data_file.content_offset})) {
			continue;
		}
		writer.RemoveManifestEntry(manifest_entry);
		manifest_entry.status = IcebergManifestEntryStatusType::DELETED;
		//! Inherits this snapshot, which deleted the file; conflict checks in other engines rely on it
		manifest_entry.SetSnapshotId(nullopt);
		removed_any_entries = true;
	}
	if (!removed_any_entries) {
		return nullopt;
	}
	return writer.WriteReplacementManifest(*loaded_manifest.manifest_metadata, std::move(rewritten_entries),
	                                       file.first_row_id);
}

void IcebergCommitState::WriteExistingManifests(IcebergSnapshotWriter &writer, int32_t schema_id,
                                                optional_ptr<const VersionedIcebergManifestDeletes> manifest_deletes) {
	for (auto &manifest : manifests) {
		if (manifest_deletes) {
			auto replacement = RewriteManifestFile(manifest, writer, *this, schema_id, *manifest_deletes);
			if (replacement) {
				writer.AddExistingManifest(std::move(*replacement));
				continue;
			}
		}
		writer.AddExistingManifest(std::move(manifest));
	}
	manifests.clear();
}

IcebergSnapshotWriter
IcebergCommitState::CreateSnapshotWriter(int32_t schema_id, IcebergSnapshotOperationType operation,
                                         optional_ptr<const VersionedIcebergManifestDeletes> manifest_deletes) {
	auto &metadata = GetTableMetadata();
	auto parent = GetLatestSnapshot();
	IcebergSnapshotWriter writer(context, metadata, schema_id, operation, next_sequence_number++, row_ids,
	                             created_metadata_files, parent);
	//! Repack the base manifests once per commit attempt, with this snapshot's identity.
	if (!written_snapshot) {
		IcebergManifestMerge::MergeManifestList(manifests, metadata.GetCurrentSchemaId(), writer, *this);
	}
	if (parent && !parent->metrics.HasTotalFilesSize()) {
		writer.SetTotalFilesSize(ReconstructTotalFilesSize(schema_id));
	}
	WriteExistingManifests(writer, schema_id, manifest_deletes);
	return writer;
}

void IcebergCommitState::AddWrittenSnapshot(IcebergWrittenSnapshot written) {
	rest_api_objects::TableUpdate table_update;
	table_update.add_snapshot_update = rest_api_objects::AddSnapshotUpdate();
	auto &update = *table_update.add_snapshot_update;
	update.base_update.action = "add-snapshot";
	update.snapshot = written.snapshot.ToRESTObject(GetTableMetadata());
	table_change.updates.push_back(std::move(table_update));

	manifests = std::move(written.manifests);
	written_snapshot.emplace(std::move(written.snapshot));
}

void IcebergCommitState::SetFormatVersion(int32_t format_version) {
	auto previous_version = GetTableMetadata().iceberg_version;
	if (format_version < previous_version) {
		throw InternalException("Cannot replay a format-version downgrade");
	}
	if (format_version == previous_version) {
		return;
	}
	if (!format_metadata) {
		format_metadata = make_uniq<IcebergTableMetadata>(table_info.table_metadata.Copy());
	}
	format_metadata->iceberg_version = format_version;
	if (previous_version < 3 && format_version >= 3) {
		// This also includes V2 manifests written by earlier snapshots in this
		// attempt. Their first row IDs belong to the new V3 manifest list only.
		AssignManifestFirstRowIds(*format_metadata, GetLatestSnapshot(), manifests, row_ids);
		format_metadata->next_row_id = row_ids.NextRowId();
	}
}

void IcebergCommitState::LoadExistingManifests(DatabaseInstance &db,
                                               vector<IcebergManifestListEntry> &&existing_manifests) {
	manifests = std::move(existing_manifests);
	auto &metadata = GetTableMetadata();
	auto current_snapshot = metadata.GetLatestSnapshot();
	if (manifests.empty() && current_snapshot) {
		IcebergSnapshotScanInfo snapshot_info;
		snapshot_info.snapshot = current_snapshot;
		snapshot_info.schema_id = metadata.GetCurrentSchemaId();

		IcebergManifestList::LoadManifestFiles(snapshot_info, metadata, context, manifests);
	}

	//! In V1 the added/deleted/existing file counts were optional
	//! But even if the attributes are present, they're allowed to be NULL
	//! In both those situations we have to read all the entries of the manifest to materialize these counts on-demand
	for (auto &manifest : manifests) {
		auto &counts = manifest.GetFile().counts;
		if (counts && counts->Complete()) {
			continue;
		}
		manifest = IcebergManifestMerge::ScanManifestEntries(manifest, *this, metadata.GetCurrentSchemaId());
	}

	AssignManifestFirstRowIds(metadata, current_snapshot, manifests, row_ids);
}

IcebergTableUpdate::IcebergTableUpdate(IcebergTableUpdateType type) : type(type) {
}

} // namespace duckdb
