#include "storage/iceberg_transaction_data.hpp"

#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_snapshot.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/catalog/iceberg_table_set.hpp"
#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "storage/table_update/common.hpp"
#include "storage/iceberg_table_information.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "avro_scan.hpp"
#include "manifest_reader.hpp"

namespace duckdb {

static int64_t NewSnapshotId() {
	auto random_number = UUID::GenerateRandomUUID().upper;
	if (random_number < 0) {
		// Flip the sign bit using XOR with 1LL shifted left 63 bits
		random_number ^= (1LL << 63);
	}
	return random_number;
}

IcebergTransactionData::IcebergTransactionData(ClientContext &context, IcebergTableInformation &table_info)
    : context(context), table_info(table_info), is_deleted(false) {
	if (table_info.table_metadata.has_next_row_id) {
		next_row_id = table_info.table_metadata.next_row_id;
	}
}

IcebergManifestFile IcebergTransactionData::CreateManifestFile(int64_t snapshot_id, sequence_number_t sequence_number,
                                                               IcebergTableMetadata &table_metadata,
                                                               IcebergManifestContentType manifest_content_type,
                                                               vector<IcebergManifestEntry> &&manifest_entries) {
	//! create manifest file path
	auto manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_file_path = table_metadata.GetMetadataPath() + "/" + manifest_file_uuid + "-m0.avro";

	// Add a manifest list entry for the delete files
	IcebergManifestFile manifest_file(manifest_file_path);
	auto &manifest = manifest_file.manifest_file;
	manifest.path = manifest_file_path;
	if (table_metadata.iceberg_version >= 3) {
		manifest_file.has_first_row_id = true;
		manifest_file.first_row_id = next_row_id;
	}

	manifest_file.manifest_path = manifest_file_path;
	manifest_file.sequence_number = sequence_number;
	manifest_file.content = manifest_content_type;
	manifest_file.added_files_count = manifest_entries.size();
	manifest_file.deleted_files_count = 0;
	manifest_file.existing_files_count = 0;
	manifest_file.added_rows_count = 0;
	manifest_file.existing_rows_count = 0;
	manifest_file.deleted_rows_count = 0;
	//! TODO: support partitions
	manifest_file.partition_spec_id = 0;
	//! manifest.partitions = CreateManifestPartition();

	//! Add the files to the manifest
	for (auto &manifest_entry : manifest_entries) {
		manifest_entry.manifest_file_path = manifest_file_path;
		auto &data_file = manifest_entry.data_file;
		switch (manifest_content_type) {
		case IcebergManifestContentType::DATA: {
			if (table_metadata.iceberg_version >= 3) {
				//! FIXME: this is required because we don't apply inheritance to uncommitted manifests
				//! But this does result in serializing this to the avro file, which *should* be NULL
				//! To fix this we should probably remove the inheritance application in the "manifest_reader"
				//! and instead do the inheritance in a path that is used by both committed and uncommitted manifests
				data_file.has_first_row_id = true;
				data_file.first_row_id = next_row_id;
				next_row_id += data_file.record_count;
			}
			manifest_file.added_rows_count += data_file.record_count;
			break;
		}
		case IcebergManifestContentType::DELETE:
			manifest_file.deleted_rows_count += data_file.record_count;
			break;
		}
		manifest_entry.sequence_number = sequence_number;
		manifest_entry.snapshot_id = snapshot_id;
		manifest_entry.partition_spec_id = manifest_file.partition_spec_id;
		if (!manifest_file.has_min_sequence_number ||
		    manifest_entry.sequence_number < manifest_file.min_sequence_number) {
			manifest_file.min_sequence_number = manifest_entry.sequence_number;
		}
		manifest_file.has_min_sequence_number = true;
	}
	manifest_file.added_snapshot_id = snapshot_id;
	manifest.entries.insert(manifest.entries.end(), std::make_move_iterator(manifest_entries.begin()),
	                        std::make_move_iterator(manifest_entries.end()));
	return manifest_file;
}

void IcebergTransactionData::AddSnapshot(IcebergSnapshotOperationType operation,
                                         vector<IcebergManifestEntry> &&data_files,
                                         case_insensitive_map_t<IcebergManifestDeletes> &&altered_manifests) {
	D_ASSERT(!data_files.empty());

	//! Generate a new snapshot id
	auto &table_metadata = table_info.table_metadata;

	CacheExistingManifestList(table_metadata);
	auto last_sequence_number = table_metadata.last_sequence_number;
	if (!alters.empty()) {
		auto &last_alter = alters.back().get();
		last_sequence_number = last_alter.snapshot.sequence_number;
	}

	auto snapshot_id = NewSnapshotId();
	auto sequence_number = last_sequence_number + 1;
	auto first_row_id = next_row_id;

	//! Construct the manifest list
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path =
	    table_metadata.GetMetadataPath() + "/snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro";

	IcebergManifestContentType manifest_content_type;
	switch (operation) {
	case IcebergSnapshotOperationType::DELETE:
		manifest_content_type = IcebergManifestContentType::DELETE;
		break;
	case IcebergSnapshotOperationType::APPEND:
		manifest_content_type = IcebergManifestContentType::DATA;
		break;
	default:
		throw NotImplementedException("Cannot have use snapshot operation type REPLACE or OVERWRITE here");
	};
	auto manifest_file =
	    CreateManifestFile(snapshot_id, sequence_number, table_metadata, manifest_content_type, std::move(data_files));

	//! Construct the snapshot
	IcebergSnapshot new_snapshot;
	new_snapshot.operation = operation;
	new_snapshot.snapshot_id = snapshot_id;
	new_snapshot.sequence_number = sequence_number;
	new_snapshot.schema_id = table_metadata.current_schema_id;
	new_snapshot.manifest_list = manifest_list_path;
	new_snapshot.timestamp_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());
	new_snapshot.has_parent_snapshot = table_metadata.has_current_snapshot || !alters.empty();
	if (new_snapshot.has_parent_snapshot) {
		if (!alters.empty()) {
			auto &last_alter = alters.back().get();
			auto &last_snapshot = last_alter.snapshot;
			new_snapshot.parent_snapshot_id = last_snapshot.snapshot_id;
		} else {
			D_ASSERT(table_metadata.has_current_snapshot);
			new_snapshot.parent_snapshot_id = table_metadata.current_snapshot_id;
		}
	}

	if (table_metadata.iceberg_version >= 3) {
		new_snapshot.has_first_row_id = true;
		new_snapshot.first_row_id = first_row_id;

		new_snapshot.has_added_rows = true;
		new_snapshot.added_rows = manifest_file.added_rows_count;
	}

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, manifest_list_path, std::move(new_snapshot));
	add_snapshot->manifest_list.AddManifestFile(std::move(manifest_file));
	add_snapshot->altered_manifests = std::move(altered_manifests);

	alters.push_back(*add_snapshot);
	updates.push_back(std::move(add_snapshot));
}

void IcebergTransactionData::CacheExistingManifestList(const IcebergTableMetadata &metadata) {
	if (!alters.empty()) {
		return;
	}

	auto current_snapshot = metadata.GetLatestSnapshot();
	if (!current_snapshot) {
		return;
	}
	auto &snapshot = *current_snapshot;

	auto &manifest_list_path = snapshot.manifest_list;
	//! Read the manifest list
	auto scan = AvroScan::ScanManifestList(snapshot, metadata, context, manifest_list_path);
	auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(*scan);
	while (!manifest_list_reader->Finished()) {
		manifest_list_reader->Read(STANDARD_VECTOR_SIZE, existing_manifest_list);
	}

	if (metadata.iceberg_version < 3) {
		return;
	}

	//! Deal with upgraded tables, if the snapshot originated from V2
	for (auto &manifest_file : existing_manifest_list) {
		if (manifest_file.content != IcebergManifestContentType::DATA) {
			continue;
		}
		if (manifest_file.has_first_row_id) {
			continue;
		}
		if (snapshot.has_first_row_id) {
			throw InternalException("Table is corrupted, snapshot has 'first-row-id' but not all 'manifest_file' "
			                        "entries have a 'first_row_id'");
		}
		manifest_file.has_first_row_id = true;
		manifest_file.first_row_id = next_row_id;
		next_row_id += manifest_file.added_rows_count;
		next_row_id += manifest_file.existing_rows_count;
	}
}

void IcebergTransactionData::AddUpdateSnapshot(vector<IcebergManifestEntry> &&delete_files,
                                               vector<IcebergManifestEntry> &&data_files,
                                               case_insensitive_map_t<IcebergManifestDeletes> &&altered_manifests) {
	//! Generate a new snapshot id
	auto &table_metadata = table_info.table_metadata;
	auto last_sequence_number = table_metadata.last_sequence_number;

	CacheExistingManifestList(table_metadata);
	if (!alters.empty()) {
		auto &last_alter = alters.back().get();
		last_sequence_number = last_alter.snapshot.sequence_number;
	}

	auto snapshot_id = NewSnapshotId();
	auto sequence_number = last_sequence_number + 1;
	auto first_row_id = next_row_id;

	//! Construct the manifest list
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path =
	    table_metadata.GetMetadataPath() + "/snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro";

	auto delete_manifest_file = CreateManifestFile(snapshot_id, sequence_number, table_metadata,
	                                               IcebergManifestContentType::DELETE, std::move(delete_files));
	// Add a manifest_file for the new insert data
	auto data_manifest_file = CreateManifestFile(snapshot_id, sequence_number, table_metadata,
	                                             IcebergManifestContentType::DATA, std::move(data_files));

	//! Construct the snapshot
	IcebergSnapshot new_snapshot;
	new_snapshot.operation = IcebergSnapshotOperationType::OVERWRITE;
	new_snapshot.snapshot_id = snapshot_id;
	new_snapshot.sequence_number = sequence_number;
	new_snapshot.schema_id = table_metadata.current_schema_id;
	new_snapshot.manifest_list = manifest_list_path;
	new_snapshot.timestamp_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());
	if (table_metadata.iceberg_version >= 3) {
		D_ASSERT(table_metadata.has_next_row_id);
		new_snapshot.has_added_rows = true;
		new_snapshot.added_rows = 0;
		for (auto &manifest_entry : data_files) {
			D_ASSERT(manifest_entry.status != IcebergManifestEntryStatusType::DELETED);
			auto &data_file = manifest_entry.data_file;
			new_snapshot.added_rows += data_file.record_count;
		}

		new_snapshot.has_first_row_id = true;
		new_snapshot.first_row_id = next_row_id;
	}

	new_snapshot.has_parent_snapshot = table_info.table_metadata.has_current_snapshot || !alters.empty();
	if (new_snapshot.has_parent_snapshot) {
		if (!alters.empty()) {
			auto &last_alter = alters.back().get();
			auto &last_snapshot = last_alter.snapshot;
			new_snapshot.parent_snapshot_id = last_snapshot.snapshot_id;
		} else {
			D_ASSERT(table_metadata.has_current_snapshot);
			new_snapshot.parent_snapshot_id = table_metadata.current_snapshot_id;
		}
	}

	if (table_metadata.iceberg_version >= 3) {
		new_snapshot.has_first_row_id = true;
		new_snapshot.first_row_id = first_row_id;

		new_snapshot.has_added_rows = true;
		new_snapshot.added_rows = data_manifest_file.added_rows_count;
	}

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, manifest_list_path, std::move(new_snapshot));
	add_snapshot->manifest_list.AddManifestFile(std::move(delete_manifest_file));
	add_snapshot->manifest_list.AddManifestFile(std::move(data_manifest_file));
	add_snapshot->altered_manifests = std::move(altered_manifests);

	alters.push_back(*add_snapshot);
	updates.push_back(std::move(add_snapshot));
}

void IcebergTransactionData::TableAddSchema() {
	updates.push_back(make_uniq<AddSchemaUpdate>(table_info));
}

void IcebergTransactionData::TableAssignUUID() {
	updates.push_back(make_uniq<AssignUUIDUpdate>(table_info));
}

void IcebergTransactionData::TableAddAssertCreate() {
	requirements.push_back(make_uniq<AssertCreateRequirement>(table_info));
}

void IcebergTransactionData::TableAddUpradeFormatVersion() {
	updates.push_back(make_uniq<UpgradeFormatVersion>(table_info));
}

void IcebergTransactionData::TableAddSetCurrentSchema() {
	updates.push_back(make_uniq<SetCurrentSchema>(table_info));
}

void IcebergTransactionData::TableAddPartitionSpec() {
	updates.push_back(make_uniq<AddPartitionSpec>(table_info));
}

void IcebergTransactionData::TableAddSortOrder() {
	updates.push_back(make_uniq<AddSortOrder>(table_info));
}

void IcebergTransactionData::TableSetDefaultSortOrder() {
	updates.push_back(make_uniq<SetDefaultSortOrder>(table_info));
}

void IcebergTransactionData::TableSetDefaultSpec() {
	updates.push_back(make_uniq<SetDefaultSpec>(table_info));
}

void IcebergTransactionData::TableSetProperties(case_insensitive_map_t<string> properties) {
	updates.push_back(make_uniq<SetProperties>(table_info, properties));
}

void IcebergTransactionData::TableRemoveProperties(vector<string> properties) {
	updates.push_back(make_uniq<RemoveProperties>(table_info, properties));
}

void IcebergTransactionData::TableSetLocation() {
	updates.push_back(make_uniq<SetLocation>(table_info));
}

} // namespace duckdb
