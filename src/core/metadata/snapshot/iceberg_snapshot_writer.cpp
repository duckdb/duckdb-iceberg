#include "core/metadata/snapshot/iceberg_snapshot_writer.hpp"

#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

static IcebergSnapshot CreateSnapshot(ClientContext &context, const IcebergTableMetadata &metadata, int32_t schema_id,
                                      IcebergSnapshotOperationType operation, sequence_number_t sequence_number,
                                      int64_t next_row_id, optional_ptr<const IcebergSnapshot> parent) {
	IcebergSnapshot snapshot(schema_id);
	snapshot.snapshot_id = IcebergSnapshot::NewSnapshotId();
	snapshot.sequence_number = sequence_number;
	snapshot.operation = operation;
	snapshot.timestamp_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());
	auto &fs = FileSystem::GetFileSystem(context);
	auto uuid = UUID::ToString(UUID::GenerateRandomUUID());
	snapshot.manifest_list = fs.JoinPath(metadata.GetMetadataPath(fs),
	                                     "snap-" + std::to_string(*snapshot.snapshot_id) + "-" + uuid + ".avro");
	if (parent) {
		snapshot.parent_snapshot_id = parent->snapshot_id;
		snapshot.metrics = IcebergSnapshotMetrics(*parent);
	}
	if (metadata.iceberg_version >= 3) {
		snapshot.first_row_id = next_row_id;
		snapshot.added_rows = 0;
	}
	return snapshot;
}

IcebergSnapshotWriter::IcebergSnapshotWriter(ClientContext &context, const IcebergTableMetadata &table_metadata,
                                             int32_t schema_id, IcebergSnapshotOperationType operation,
                                             sequence_number_t sequence_number, int64_t next_row_id,
                                             vector<string> &created_metadata_files,
                                             optional_ptr<const IcebergSnapshot> parent)
    : context(context), table_metadata(table_metadata), db(DatabaseInstance::GetDatabase(context)),
      avro_copy(IcebergUtils::GetCopyFunction(context, "avro").function),
      created_metadata_files(created_metadata_files),
      snapshot(CreateSnapshot(context, table_metadata, schema_id, operation, sequence_number, next_row_id, parent)),
      manifest_list(snapshot.manifest_list), next_row_id(next_row_id) {
}

void IcebergSnapshotWriter::AddExistingManifest(IcebergManifestListEntry manifest) {
	if (!manifest.HasFile()) {
		throw InternalException("Cannot carry forward unwritten manifest content");
	}
	manifest_list.AddExistingManifestFile(std::move(manifest));
}

void IcebergSnapshotWriter::WriteManifestFile(IcebergManifestListEntry &list_entry) {
	if (list_entry.GetManifestEntries().empty()) {
		throw InternalException("Cannot write an empty Iceberg manifest");
	}
	auto &manifest = list_entry.GetManifest();
	auto &fs = FileSystem::GetFileSystem(context);
	auto path =
	    fs.JoinPath(table_metadata.GetMetadataPath(fs), UUID::ToString(UUID::GenerateRandomUUID()) + "-m0.avro");
	if (!manifest.min_sequence_number || *manifest.min_sequence_number > manifest.sequence_number) {
		manifest.min_sequence_number = manifest.sequence_number;
	}
	created_metadata_files.push_back(path);
	auto length = manifest_file::WriteToFile(table_metadata, list_entry, path, avro_copy, db, context);
	list_entry.SetFile(std::move(path), length, *snapshot.snapshot_id);
}

void IcebergSnapshotWriter::WriteManifest(const IcebergPendingManifest &pending) {
	optional<int64_t> first_row_id;
	if (table_metadata.iceberg_version >= 3 && pending.GetMetadata().content == IcebergManifestContentType::DATA) {
		first_row_id = next_row_id;
	}
	auto entries = pending.GetEntries();
	auto manifest = IcebergManifestListEntry::CreateFromEntries(
	    *snapshot.sequence_number, table_metadata, pending.GetMetadata(), std::move(entries), first_row_id);
	snapshot.metrics.AddManifestListEntry(manifest);
	WriteManifestFile(manifest);
	if (first_row_id) {
		auto &counts = *manifest.GetFile().counts;
		next_row_id += *counts.existing_rows_count + *counts.added_rows_count;
		*snapshot.added_rows += *counts.added_rows_count;
	}
	manifest_list.AddExistingManifestFile(std::move(manifest));
}

IcebergManifestListEntry IcebergSnapshotWriter::WriteReplacementManifest(const IcebergManifestMetadata &metadata,
                                                                         vector<IcebergManifestEntry> entries,
                                                                         optional<int64_t> first_row_id) {
	auto manifest = IcebergManifestListEntry::CreateFromEntries(*snapshot.sequence_number, table_metadata, metadata,
	                                                            std::move(entries), first_row_id);
	WriteManifestFile(manifest);
	return manifest;
}

void IcebergSnapshotWriter::RemoveManifestEntry(const IcebergManifestEntry &entry) {
	snapshot.metrics.RemoveManifestEntry(entry);
}

void IcebergSnapshotWriter::SetTotalFilesSize(int64_t total_files_size) {
	snapshot.metrics.SetTotalFilesSize(total_files_size);
}

IcebergWrittenSnapshot IcebergWrittenSnapshot::Create(IcebergSnapshotWriter writer) {
	writer.created_metadata_files.push_back(writer.snapshot.manifest_list);
	manifest_list::WriteToFile(writer.table_metadata, writer.manifest_list, writer.avro_copy, writer.db,
	                           writer.context);
	return {std::move(writer.snapshot), writer.manifest_list.GetManifestListEntries(), writer.next_row_id};
}

} // namespace duckdb
