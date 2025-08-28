#include "storage/iceberg_transaction_data.hpp"
#include "storage/irc_table_set.hpp"
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

void IcebergTransactionData::AddSnapshot(IcebergSnapshotOperationType operation,
                                         vector<IcebergManifestEntry> &&data_files) {
	//! Generate a new snapshot id
	auto &table_metadata = table_info.table_metadata;
	auto snapshot_id = NewSnapshotId();

	auto last_sequence_number = table_metadata.last_sequence_number;
	if (!alters.empty()) {
		auto &last_alter = alters.back().get();
		last_sequence_number = last_alter.snapshot.sequence_number;
	}

	auto sequence_number = last_sequence_number + 1;

	//! Construct the manifest file
	auto manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_file_path = table_info.BaseFilePath() + "/metadata/" + manifest_file_uuid + "-m0.avro";
	IcebergManifestFile new_manifest_file(manifest_file_path);

	//! Construct the manifest list
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path = table_info.BaseFilePath() + "/metadata/snap-" + std::to_string(snapshot_id) + "-" +
	                          manifest_list_uuid + ".avro";

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

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, std::move(new_manifest_file), manifest_list_path,
	                                                  std::move(new_snapshot));
	auto &manifest_file = add_snapshot->manifest_file;
	auto &manifest = add_snapshot->manifest;
	auto &snapshot = add_snapshot->snapshot;

	manifest.manifest_path = manifest_file_path;
	manifest.sequence_number = sequence_number;
	if (operation == IcebergSnapshotOperationType::APPEND) {
		manifest.content = IcebergManifestContentType::DATA;
	} else if (operation == IcebergSnapshotOperationType::DELETE) {
		manifest.content = IcebergManifestContentType::DELETE;
	}
	manifest.added_files_count = data_files.size();
	manifest.deleted_files_count = 0;
	manifest.existing_files_count = 0;
	manifest.added_rows_count = 0;
	manifest.existing_rows_count = 0;
	//! TODO: support partitions
	manifest.partition_spec_id = 0;
	//! manifest.partitions = CreateManifestPartition();

	//! Add the data files
	for (auto &data_file : data_files) {
		if (operation == IcebergSnapshotOperationType::APPEND) {
			manifest.added_rows_count += data_file.record_count;
		} else if (operation == IcebergSnapshotOperationType::DELETE) {
			manifest.deleted_rows_count += data_file.record_count;
		}
		data_file.sequence_number = snapshot.sequence_number;
		data_file.snapshot_id = snapshot.snapshot_id;
		data_file.partition_spec_id = manifest.partition_spec_id;
		if (!manifest.has_min_sequence_number || data_file.sequence_number < manifest.min_sequence_number) {
			manifest.min_sequence_number = data_file.sequence_number;
		}
		manifest.has_min_sequence_number = true;
	}
	manifest.added_snapshot_id = snapshot.snapshot_id;
	manifest_file.data_files.insert(manifest_file.data_files.end(), std::make_move_iterator(data_files.begin()),
	                                std::make_move_iterator(data_files.end()));
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

void IcebergTransactionData::TableSetLocation() {
	updates.push_back(make_uniq<SetLocation>(table_info));
}

} // namespace duckdb
