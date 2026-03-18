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

IcebergTransactionData::IcebergTransactionData(ClientContext &context, const IcebergTableInformation &table_info)
    : context(context), table_info(table_info), is_deleted(false) {
	if (table_info.table_metadata.has_next_row_id) {
		next_row_id = table_info.table_metadata.next_row_id;
	}
}

void IcebergTransactionData::CacheExistingManifestList(lock_guard<mutex> &guard, const IcebergTableMetadata &metadata) {
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
	for (auto &manifest_list_entry : existing_manifest_list) {
		auto &manifest_file = manifest_list_entry.file;
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

void IcebergTransactionData::AddSnapshot(IcebergSnapshotOperationType operation,
                                         vector<IcebergManifestEntry> &&data_files,
                                         case_insensitive_map_t<IcebergManifestDeletes> &&altered_manifests) {
	//! NOTE: Lock has to be held to make sure the rows are assigned the correct row ids
	lock_guard<mutex> guard(lock);

	//! Generate a new snapshot id
	auto &table_metadata = table_info.table_metadata;
	auto last_sequence_number = table_metadata.last_sequence_number;

	CacheExistingManifestList(guard, table_metadata);
	if (!alters.empty()) {
		auto &last_alter = alters.back().get();
		last_sequence_number = last_alter.snapshot.sequence_number;
	}

	auto snapshot_id = IcebergSnapshot::NewSnapshotId();
	const auto sequence_number = last_sequence_number + 1;
	const auto first_row_id = next_row_id;

	//! Construct the manifest list
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_path = fs.JoinPath(table_metadata.GetMetadataPath(fs),
	                                      "snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro");

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
	auto manifest_file = IcebergManifestListEntry::CreateFromEntries(
	    fs, snapshot_id, sequence_number, table_metadata, manifest_content_type, std::move(data_files), next_row_id);

	//! Construct the snapshot
	IcebergSnapshot new_snapshot;
	new_snapshot.operation = operation;
	new_snapshot.snapshot_id = snapshot_id;
	new_snapshot.sequence_number = sequence_number;
	new_snapshot.schema_id = table_metadata.current_schema_id;
	new_snapshot.manifest_list = manifest_list_path;
	new_snapshot.timestamp_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());

	new_snapshot.has_parent_snapshot = table_metadata.has_current_snapshot || !alters.empty();
	optional_ptr<const IcebergSnapshot> parent_snapshot;
	if (new_snapshot.has_parent_snapshot) {
		if (!alters.empty()) {
			auto &last_alter = alters.back().get();
			parent_snapshot = last_alter.snapshot;
		} else {
			D_ASSERT(table_metadata.has_current_snapshot);
			parent_snapshot = table_metadata.GetSnapshotById(table_metadata.current_snapshot_id);
		}
	}

	if (parent_snapshot) {
		new_snapshot.metrics = IcebergSnapshotMetrics(*parent_snapshot);
		new_snapshot.parent_snapshot_id = parent_snapshot->snapshot_id;
	}
	new_snapshot.metrics.AddManifestFile(manifest_file.file);

	if (table_metadata.iceberg_version >= 3) {
		new_snapshot.has_first_row_id = true;
		new_snapshot.first_row_id = first_row_id;

		new_snapshot.has_added_rows = true;
		if (manifest_file.file.content == IcebergManifestContentType::DATA) {
			new_snapshot.added_rows = manifest_file.file.added_rows_count;
		} else {
			new_snapshot.added_rows = 0;
		}
	}

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, manifest_list_path, std::move(new_snapshot));
	add_snapshot->manifest_list.AddManifestFile(std::move(manifest_file));
	// make sure we are still inserting into the current schema
	if (table_metadata.has_current_snapshot) {
		TableAddAssertCurrentSchemaId();
	}
	add_snapshot->altered_manifests = std::move(altered_manifests);

	alters.push_back(*add_snapshot);
	updates.push_back(std::move(add_snapshot));
}

void IcebergTransactionData::AddUpdateSnapshot(vector<IcebergManifestEntry> &&delete_files,
                                               vector<IcebergManifestEntry> &&data_files,
                                               case_insensitive_map_t<IcebergManifestDeletes> &&altered_manifests) {
	//! NOTE: Lock has to be held to make sure the rows are assigned the correct row ids
	lock_guard<mutex> guard(lock);

	//! Generate a new snapshot id
	auto &table_metadata = table_info.table_metadata;
	auto last_sequence_number = table_metadata.last_sequence_number;

	CacheExistingManifestList(guard, table_metadata);
	if (!alters.empty()) {
		auto &last_alter = alters.back().get();
		last_sequence_number = last_alter.snapshot.sequence_number;
	}

	auto snapshot_id = IcebergSnapshot::NewSnapshotId();
	const auto sequence_number = last_sequence_number + 1;
	const auto first_row_id = next_row_id;

	auto &fs = FileSystem::GetFileSystem(context);
	//! Construct the manifest list
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path = fs.JoinPath(table_metadata.GetMetadataPath(fs),
	                                      "snap-" + std::to_string(snapshot_id) + "-" + manifest_list_uuid + ".avro");

	auto delete_manifest_file = IcebergManifestListEntry::CreateFromEntries(
	    fs, snapshot_id, sequence_number, table_metadata, IcebergManifestContentType::DELETE, std::move(delete_files),
	    next_row_id);
	// Add a manifest_file for the new insert data
	auto data_manifest_file = IcebergManifestListEntry::CreateFromEntries(
	    fs, snapshot_id, sequence_number, table_metadata, IcebergManifestContentType::DATA, std::move(data_files),
	    next_row_id);

	//! Construct the snapshot
	IcebergSnapshot new_snapshot;
	new_snapshot.operation = IcebergSnapshotOperationType::OVERWRITE;
	new_snapshot.snapshot_id = snapshot_id;
	new_snapshot.sequence_number = sequence_number;
	new_snapshot.schema_id = table_metadata.current_schema_id;
	new_snapshot.manifest_list = manifest_list_path;
	new_snapshot.timestamp_ms = Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());

	new_snapshot.has_parent_snapshot = table_metadata.has_current_snapshot || !alters.empty();
	optional_ptr<const IcebergSnapshot> parent_snapshot;
	if (new_snapshot.has_parent_snapshot) {
		if (!alters.empty()) {
			auto &last_alter = alters.back().get();
			parent_snapshot = last_alter.snapshot;
		} else {
			D_ASSERT(table_metadata.has_current_snapshot);
			parent_snapshot = table_metadata.GetSnapshotById(table_metadata.current_snapshot_id);
		}
	}

	if (parent_snapshot) {
		new_snapshot.metrics = IcebergSnapshotMetrics(*parent_snapshot);
		new_snapshot.parent_snapshot_id = parent_snapshot->snapshot_id;
	}
	new_snapshot.metrics.AddManifestFile(data_manifest_file.file);
	new_snapshot.metrics.AddManifestFile(delete_manifest_file.file);

	if (table_metadata.iceberg_version >= 3) {
		new_snapshot.has_first_row_id = true;
		new_snapshot.first_row_id = first_row_id;

		new_snapshot.has_added_rows = true;
		new_snapshot.added_rows = data_manifest_file.file.added_rows_count;
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

void IcebergTransactionData::TableAddAssertCurrentSchemaId() {
	requirements.push_back(make_uniq<AssertCurrentSchemaIdRequirement>(table_info));
}

void IcebergTransactionData::TableAddAssertLastAssignedColumnFieldId() {
	requirements.push_back(make_uniq<AssertLastAssignedColumnFieldIdRequirement>(table_info));
}

void IcebergTransactionData::TableAddAssertLastAssignedPartitionId() {
	requirements.push_back(make_uniq<AssertLastAssignedPartitionIdRequirement>(table_info));
}

void IcebergTransactionData::TableAddAssertDefaultSpecId() {
	requirements.push_back(make_uniq<AssertDefaultSpecIdRequirement>(table_info));
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

void IcebergTransactionData::TableSetProperties(const case_insensitive_map_t<string> &properties) {
	updates.push_back(make_uniq<SetProperties>(table_info, properties));
}

void IcebergTransactionData::TableRemoveProperties(const vector<string> &properties) {
	updates.push_back(make_uniq<RemoveProperties>(table_info, properties));
}

void IcebergTransactionData::TableSetLocation() {
	updates.push_back(make_uniq<SetLocation>(table_info));
}

} // namespace duckdb
