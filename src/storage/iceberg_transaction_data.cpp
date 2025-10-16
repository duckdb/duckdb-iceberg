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

static const IcebergSnapshot::metrics_map_t kMetricsWithEmptyTotals {{SnapshotMetricType::TOTAL_DATA_FILES, 0},
                                                                     {SnapshotMetricType::TOTAL_RECORDS, 0}};

static IcebergSnapshot::metrics_map_t GetSnapshotMetrics(const IcebergManifest &manifest,
                                                         const IcebergSnapshot::metrics_map_t &previous_metrics) {
	IcebergSnapshot::metrics_map_t metrics {{SnapshotMetricType::ADDED_DATA_FILES, manifest.added_files_count},
	                                        {SnapshotMetricType::ADDED_RECORDS, manifest.added_rows_count},
	                                        {SnapshotMetricType::DELETED_DATA_FILES, manifest.deleted_files_count},
	                                        {SnapshotMetricType::DELETED_RECORDS, manifest.deleted_rows_count}};

	auto previous_total_files = previous_metrics.find(SnapshotMetricType::TOTAL_DATA_FILES);
	if (previous_total_files != previous_metrics.end()) {
		int64_t total_files = previous_total_files->second + manifest.added_files_count - manifest.deleted_files_count;
		if (total_files >= 0)
			metrics[SnapshotMetricType::TOTAL_DATA_FILES] = total_files;
	}

	auto previous_total_records = previous_metrics.find(SnapshotMetricType::TOTAL_RECORDS);
	if (previous_total_records != previous_metrics.end()) {
		int64_t total_records =
		    previous_total_records->second + manifest.added_rows_count - manifest.deleted_rows_count;
		if (total_records >= 0) {
			metrics[SnapshotMetricType::TOTAL_RECORDS] = total_records;
		}
	}

	return metrics;
}

void IcebergTransactionData::AddSnapshot(IcebergSnapshotOperationType operation,
                                         vector<IcebergManifestEntry> &&data_files) {
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

	//! Construct the manifest file
	auto manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_file_path = table_info.BaseFilePath() + "/metadata/" + manifest_file_uuid + "-m0.avro";
	IcebergManifestFile new_manifest_file(manifest_file_path);

	//! Construct the manifest list
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path = table_info.BaseFilePath() + "/metadata/snap-" + std::to_string(snapshot_id) + "-" +
	                          manifest_list_uuid + ".avro";

	//! Construct the manifest
	IcebergManifest manifest;
	manifest.manifest_path = manifest_file_path;
	manifest.sequence_number = sequence_number;
	manifest.content = IcebergManifestContentType::DATA;
	manifest.added_files_count = data_files.size();
	manifest.existing_files_count = 0;
	manifest.deleted_files_count = 0;
	manifest.added_rows_count = 0;
	manifest.existing_rows_count = 0;
	manifest.deleted_rows_count = 0;
	//! TODO: support partitions
	manifest.partition_spec_id = 0;
	//! manifest.partitions = CreateManifestPartition();

	//! Add the data files
	for (auto &data_file : data_files) {
		manifest.added_rows_count += data_file.record_count;
		data_file.sequence_number = sequence_number;
		if (!manifest.has_min_sequence_number || data_file.sequence_number < manifest.min_sequence_number) {
			manifest.min_sequence_number = data_file.sequence_number;
		}
		manifest.has_min_sequence_number = true;
	}
	manifest.added_snapshot_id = snapshot_id;

	//! Construct the snapshot
	IcebergSnapshot new_snapshot;
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
			new_snapshot.metrics = GetSnapshotMetrics(manifest, last_alter.snapshot.metrics);
		} else {
			D_ASSERT(table_info.table_metadata.has_current_snapshot);
			new_snapshot.parent_snapshot_id = table_info.table_metadata.current_snapshot_id;
			new_snapshot.metrics = GetSnapshotMetrics(
			    manifest, table_info.table_metadata.GetSnapshotById(new_snapshot.parent_snapshot_id)->metrics);
		}
	} else {
		// If there was no previous snapshot, default the metrics to start totals at 0
		new_snapshot.metrics = GetSnapshotMetrics(manifest, kMetricsWithEmptyTotals);
	}

	new_manifest_file.data_files.insert(new_manifest_file.data_files.end(), std::make_move_iterator(data_files.begin()),
	                                    std::make_move_iterator(data_files.end()));
	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, std::move(new_manifest_file), manifest_list_path,
	                                                  std::move(new_snapshot));
	add_snapshot->manifest = std::move(manifest);

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
