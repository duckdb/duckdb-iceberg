#include "core/metadata/snapshot/iceberg_snapshot_writer.hpp"

#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

static IcebergSnapshot CreateSnapshot(ClientContext &context, const IcebergTableMetadata &metadata, int32_t schema_id,
                                      IcebergSnapshotOperationType operation, sequence_number_t sequence_number,
                                      optional_ptr<const IcebergSnapshot> parent) {
	IcebergSnapshot snapshot(schema_id, IcebergSnapshot::NewSnapshotId());
	snapshot.sequence_number = sequence_number;
	snapshot.operation = operation;
	snapshot.timestamp_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());
	auto &fs = FileSystem::GetFileSystem(context);
	auto uuid = UUID::ToString(UUID::GenerateRandomUUID());
	snapshot.manifest_list = fs.JoinPath(metadata.GetMetadataPath(fs),
	                                     "snap-" + std::to_string(snapshot.snapshot_id) + "-" + uuid + ".avro");
	if (parent) {
		snapshot.parent_snapshot_id = parent->snapshot_id;
		snapshot.metrics = IcebergSnapshotMetrics(*parent);
	}
	return snapshot;
}

IcebergSnapshotWriter::IcebergSnapshotWriter(ClientContext &context, const IcebergTableMetadata &table_metadata,
                                             int32_t schema_id, IcebergSnapshotOperationType operation,
                                             sequence_number_t sequence_number, IcebergRowIdAllocator &row_ids,
                                             vector<string> &created_metadata_files,
                                             optional_ptr<const IcebergSnapshot> parent)
    : context(context), table_metadata(table_metadata), db(DatabaseInstance::GetDatabase(context)),
      avro_copy(IcebergUtils::GetCopyFunction(context, "avro").function),
      created_metadata_files(created_metadata_files),
      snapshot(CreateSnapshot(context, table_metadata, schema_id, operation, sequence_number, parent)),
      manifest_list(snapshot.manifest_list), row_ids(row_ids) {
}

void IcebergSnapshotWriter::AddExistingManifest(IcebergManifestListEntry manifest) {
	if (table_metadata.iceberg_version >= 3) {
		row_ids.AssignExistingManifest(manifest.GetManifest());
	}
	manifest_list.AddExistingManifestFile(std::move(manifest));
}

IcebergManifestMetadata IcebergSnapshotWriter::GetWriteMetadata(const IcebergManifestMetadata &source) const {
	return IcebergManifestMetadata(source.schema_id, source.partition_spec_id,
	                               NumericCast<int32_t>(table_metadata.iceberg_version), source.content);
}

IcebergManifestListEntry IcebergSnapshotWriter::WriteManifestFile(const IcebergManifestMetadata &source_metadata,
                                                                  vector<IcebergManifestEntry> entries,
                                                                  optional<int64_t> first_row_id) {
	if (entries.empty()) {
		throw InternalException("Cannot write an empty Iceberg manifest");
	}
	auto metadata = GetWriteMetadata(source_metadata);
	auto manifest =
	    IcebergManifest::CreateFromEntries(*snapshot.sequence_number, table_metadata, metadata, entries, first_row_id);
	auto &fs = FileSystem::GetFileSystem(context);
	auto path =
	    fs.JoinPath(table_metadata.GetMetadataPath(fs), UUID::ToString(UUID::GenerateRandomUUID()) + "-m0.avro");
	if (!manifest.min_sequence_number || *manifest.min_sequence_number > manifest.sequence_number) {
		manifest.min_sequence_number = manifest.sequence_number;
	}
	created_metadata_files.push_back(path);
	auto length = manifest_file::WriteToFile(table_metadata, metadata, entries, path, avro_copy, db, context);
	IcebergManifestFile file(std::move(path), length, snapshot.snapshot_id, std::move(manifest));
	return IcebergManifestListEntry(std::move(file), std::move(metadata), std::move(entries));
}

void IcebergSnapshotWriter::WriteManifest(const IcebergPendingManifest &pending) {
	optional<int64_t> first_row_id;
	if (table_metadata.iceberg_version >= 3) {
		first_row_id = row_ids.AllocateManifest(pending);
	}
	auto manifest = WriteManifestFile(pending.GetMetadata(), pending.GetEntries(), first_row_id);
	snapshot.metrics.AddManifestEntries(manifest.GetFile().content, manifest.GetManifestEntries());
	AddExistingManifest(std::move(manifest));
}

IcebergManifestListEntry IcebergSnapshotWriter::WriteReplacementManifest(const IcebergManifestMetadata &metadata,
                                                                         vector<IcebergManifestEntry> entries,
                                                                         optional<int64_t> first_row_id) {
	return WriteManifestFile(metadata, std::move(entries), first_row_id);
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
	if (writer.table_metadata.iceberg_version >= 3) {
		writer.row_ids.CompleteSnapshot(writer.snapshot);
	}
	return {std::move(writer.snapshot), writer.manifest_list.TakeManifestListEntries()};
}

} // namespace duckdb
