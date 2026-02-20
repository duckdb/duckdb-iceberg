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

void IcebergTransactionData::CreateManifestListEntry(IcebergAddSnapshot &add_snapshot,
                                                     IcebergTableMetadata &table_metadata,
                                                     IcebergManifestContentType manifest_content_type,
                                                     vector<IcebergManifestEntry> &&manifest_entries) {
	//! create manifest file path
	auto manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_file_path = table_metadata.GetMetadataPath() + "/" + manifest_file_uuid + "-m0.avro";

	// Add a manifest list entry for the delete files
	auto &manifest_list_delete_entry = add_snapshot.manifest_list.CreateNewManifestListEntry(manifest_file_path);
	auto &manifest_file = manifest_list_delete_entry.manifest_file;
	if (table_metadata.iceberg_version >= 3) {
		manifest_list_delete_entry.has_first_row_id = true;
		manifest_list_delete_entry.first_row_id = next_row_id;
	}

	manifest_file.path = manifest_file_path;

	// auto &manifest = add_snapshot->manifest;
	auto &snapshot = add_snapshot.snapshot;
	manifest_list_delete_entry.manifest_path = manifest_file_path;
	manifest_list_delete_entry.sequence_number = snapshot.sequence_number;
	manifest_list_delete_entry.content = manifest_content_type;
	manifest_list_delete_entry.added_files_count = manifest_entries.size();
	manifest_list_delete_entry.deleted_files_count = 0;
	manifest_list_delete_entry.existing_files_count = 0;
	manifest_list_delete_entry.added_rows_count = 0;
	manifest_list_delete_entry.deleted_rows_count = 0;
	manifest_list_delete_entry.existing_rows_count = 0;
	//! TODO: support partitions
	manifest_list_delete_entry.partition_spec_id = 0;
	//! manifest.partitions = CreateManifestPartition();

	//! Add the delete files to the manifest
	for (auto &manifest_entry : manifest_entries) {
		manifest_entry.manifest_file_path = manifest_file_path;
		auto &data_file = manifest_entry.data_file;
		switch (manifest_content_type) {
		case IcebergManifestContentType::DATA:
			manifest_list_delete_entry.added_rows_count += data_file.record_count;
			break;
		case IcebergManifestContentType::DELETE:
			manifest_list_delete_entry.deleted_rows_count += data_file.record_count;
			break;
		}
		manifest_entry.sequence_number = snapshot.sequence_number;
		manifest_entry.snapshot_id = snapshot.snapshot_id;
		manifest_entry.partition_spec_id = manifest_list_delete_entry.partition_spec_id;
		if (!manifest_list_delete_entry.has_min_sequence_number ||
		    manifest_entry.sequence_number < manifest_list_delete_entry.min_sequence_number) {
			manifest_list_delete_entry.min_sequence_number = manifest_entry.sequence_number;
		}
		manifest_list_delete_entry.has_min_sequence_number = true;
	}
	manifest_list_delete_entry.added_snapshot_id = snapshot.snapshot_id;
	manifest_file.entries.insert(manifest_file.entries.end(), std::make_move_iterator(manifest_entries.begin()),
	                             std::make_move_iterator(manifest_entries.end()));
}

void IcebergTransactionData::AddSnapshot(IcebergSnapshotOperationType operation,
                                         vector<IcebergManifestEntry> &&data_files,
                                         case_insensitive_map_t<IcebergManifestDeletes> &&altered_manifests) {
	D_ASSERT(!data_files.empty());

	//! Generate a new snapshot id
	auto &table_metadata = table_info.table_metadata;
	auto snapshot_id = NewSnapshotId();

	auto last_sequence_number = table_metadata.last_sequence_number;
	if (!alters.empty()) {
		auto &last_alter = alters.back().get();
		last_sequence_number = last_alter.snapshot.sequence_number;
	}

	auto sequence_number = last_sequence_number + 1;

	//! Construct the manifest list
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path =
	    table_metadata.GetMetadataPath() + "/snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro";

	//! Construct the snapshot
	IcebergSnapshot new_snapshot;
	new_snapshot.operation = operation;
	new_snapshot.snapshot_id = snapshot_id;
	new_snapshot.sequence_number = sequence_number;
	new_snapshot.schema_id = table_metadata.current_schema_id;
	new_snapshot.manifest_list = manifest_list_path;
	new_snapshot.operation = operation;
	new_snapshot.timestamp_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());

	new_snapshot.has_parent_snapshot = table_info.table_metadata.has_current_snapshot || !alters.empty();
	if (new_snapshot.has_parent_snapshot) {
		if (!alters.empty()) {
			auto &last_alter = alters.back().get();
			new_snapshot.parent_snapshot_id = last_alter.snapshot.snapshot_id;
		} else {
			D_ASSERT(table_info.table_metadata.has_current_snapshot);
			new_snapshot.parent_snapshot_id = table_info.table_metadata.current_snapshot_id;
		}
	}

	auto manifest_content_type = IcebergManifestContentType::DATA;
	switch (operation) {
	case IcebergSnapshotOperationType::DELETE:
		if (table_metadata.has_next_row_id) {
			//! IRC requires this for a successful commit..
			new_snapshot.has_added_rows = true;
			new_snapshot.added_rows = 0;
		}
		manifest_content_type = IcebergManifestContentType::DELETE;
		break;
	case IcebergSnapshotOperationType::APPEND: {
		manifest_content_type = IcebergManifestContentType::DATA;
		if (table_metadata.has_next_row_id) {
			new_snapshot.has_added_rows = true;
			new_snapshot.added_rows = 0;

			for (auto &manifest_entry : data_files) {
				D_ASSERT(manifest_entry.status != IcebergManifestEntryStatusType::DELETED);
				auto &data_file = manifest_entry.data_file;
				//! Since we don't create the avro files yet,
				//! we don't pass the logic that assigns the 'first_row_id' to scanned entries
				//! So we do it here - only problem now is that this gets serialized to the file upon commit...
				data_file.has_first_row_id = true;
				data_file.first_row_id = next_row_id + new_snapshot.added_rows;
				new_snapshot.added_rows += data_file.record_count;
			}
		}
		break;
	}
	default:
		throw NotImplementedException("Cannot have use snapshot operation type REPLACE or OVERWRITE here");
	}
	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, manifest_list_path, std::move(new_snapshot));
	CreateManifestListEntry(*add_snapshot, table_metadata, manifest_content_type, std::move(data_files));
	add_snapshot->altered_manifests = std::move(altered_manifests);

	if (table_metadata.iceberg_version >= 3) {
		auto &snapshot = add_snapshot->snapshot;
		snapshot.has_first_row_id = true;
		snapshot.first_row_id = next_row_id;

		auto &last_manifest_file = add_snapshot->manifest_list.manifest_entries.back();
		snapshot.has_added_rows = true;
		snapshot.added_rows = last_manifest_file.added_rows_count;
		next_row_id += last_manifest_file.added_rows_count;
	}
	alters.push_back(*add_snapshot);
	updates.push_back(std::move(add_snapshot));
}

void IcebergTransactionData::AddUpdateSnapshot(vector<IcebergManifestEntry> &&delete_files,
                                               vector<IcebergManifestEntry> &&data_files,
                                               case_insensitive_map_t<IcebergManifestDeletes> &&altered_manifests) {
	//! Generate a new snapshot id
	auto &table_metadata = table_info.table_metadata;
	auto snapshot_id = NewSnapshotId();

	auto last_sequence_number = table_metadata.last_sequence_number;
	if (!alters.empty()) {
		auto &last_alter = alters.back().get();
		last_sequence_number = last_alter.snapshot.sequence_number;
	}

	auto sequence_number = last_sequence_number + 1;

	//! Construct the manifest list
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path =
	    table_metadata.GetMetadataPath() + "/snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro";

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
			new_snapshot.parent_snapshot_id = last_alter.snapshot.snapshot_id;
		} else {
			D_ASSERT(table_info.table_metadata.has_current_snapshot);
			new_snapshot.parent_snapshot_id = table_info.table_metadata.current_snapshot_id;
		}
	}

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, manifest_list_path, std::move(new_snapshot));
	CreateManifestListEntry(*add_snapshot, table_metadata, IcebergManifestContentType::DELETE, std::move(delete_files));

	// Add a manifest list entry for the new insert data
	CreateManifestListEntry(*add_snapshot, table_metadata, IcebergManifestContentType::DATA, std::move(data_files));
	add_snapshot->altered_manifests = std::move(altered_manifests);

	if (table_metadata.iceberg_version >= 3) {
		auto &snapshot = add_snapshot->snapshot;
		snapshot.has_first_row_id = true;
		snapshot.first_row_id = next_row_id;

		auto &last_manifest_file = add_snapshot->manifest_list.manifest_entries.back();
		snapshot.has_added_rows = true;
		snapshot.added_rows = last_manifest_file.added_rows_count;
		next_row_id += last_manifest_file.added_rows_count;
	}

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
