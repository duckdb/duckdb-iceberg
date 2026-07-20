#include "core/metadata/snapshot/iceberg_snapshot.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "common/iceberg_utils.hpp"

namespace duckdb {

int64_t IcebergSnapshot::NewSnapshotId() {
	auto random_number = UUID::GenerateRandomUUID().upper;
	if (random_number < 0) {
		// Flip the sign bit using XOR with 1LL shifted left 63 bits
		random_number ^= (1LL << 63);
	}
	return random_number;
}

static map<IcebergSnapshotMetricType, int64_t> EmptyMetrics() {
	return map<IcebergSnapshotMetricType, int64_t>({{IcebergSnapshotMetricType::TOTAL_DATA_FILES, 0},
	                                                {IcebergSnapshotMetricType::TOTAL_RECORDS, 0},
	                                                {IcebergSnapshotMetricType::TOTAL_DELETE_FILES, 0},
	                                                {IcebergSnapshotMetricType::TOTAL_POSITION_DELETES, 0},
	                                                {IcebergSnapshotMetricType::TOTAL_FILES_SIZE, 0}});
}

IcebergSnapshotMetrics::IcebergSnapshotMetrics() : metrics(EmptyMetrics()) {
}

IcebergSnapshotMetrics::IcebergSnapshotMetrics(const IcebergSnapshot &snapshot) {
	auto &other_metrics = snapshot.metrics.metrics;
	auto total_data_files = other_metrics.find(IcebergSnapshotMetricType::TOTAL_DATA_FILES);
	if (total_data_files != other_metrics.end()) {
		metrics[IcebergSnapshotMetricType::TOTAL_DATA_FILES] = total_data_files->second;
	}
	auto total_records = other_metrics.find(IcebergSnapshotMetricType::TOTAL_RECORDS);
	if (total_records != other_metrics.end()) {
		metrics[IcebergSnapshotMetricType::TOTAL_RECORDS] = total_records->second;
	}
	auto total_delete_files = other_metrics.find(IcebergSnapshotMetricType::TOTAL_DELETE_FILES);
	if (total_delete_files != other_metrics.end()) {
		metrics[IcebergSnapshotMetricType::TOTAL_DELETE_FILES] = total_delete_files->second;
	}
	auto total_position_deletes = other_metrics.find(IcebergSnapshotMetricType::TOTAL_POSITION_DELETES);
	if (total_position_deletes != other_metrics.end()) {
		metrics[IcebergSnapshotMetricType::TOTAL_POSITION_DELETES] = total_position_deletes->second;
	}
	auto total_files_size = other_metrics.find(IcebergSnapshotMetricType::TOTAL_FILES_SIZE);
	if (total_files_size != other_metrics.end()) {
		metrics[IcebergSnapshotMetricType::TOTAL_FILES_SIZE] = total_files_size->second;
	}
}

void IcebergSnapshotMetrics::AddSizeMetric(IcebergSnapshotMetricType type, int64_t value) {
	auto &metric = metrics.emplace(type, 0).first->second;
	metric = IcebergUtils::AddFileSizeChecked(metric, value);
}

void IcebergSnapshotMetrics::UpdateTotalFilesSize(int64_t added, int64_t removed) {
	auto total_it = metrics.find(IcebergSnapshotMetricType::TOTAL_FILES_SIZE);
	if (total_it == metrics.end()) {
		return;
	}
	int64_t with_added;
	int64_t updated;
	if (!TryAddOperator::Operation(total_it->second, added, with_added) ||
	    !TrySubtractOperator::Operation(with_added, removed, updated)) {
		throw InvalidConfigurationException("Iceberg snapshot 'total-files-size' exceeds the supported BIGINT range");
	}
	if (updated < 0) {
		throw InvalidConfigurationException("Iceberg snapshot 'total-files-size' cannot be negative");
	}
	total_it->second = updated;
}

void IcebergSnapshotMetrics::AddManifestListEntry(const IcebergManifestListEntry &manifest_list_entry) {
	int64_t added_files_size = 0;
	int64_t deleted_files_size = 0;
	for (const auto &manifest_entry : manifest_list_entry.GetManifestEntries()) {
		switch (manifest_entry.status) {
		case IcebergManifestEntryStatusType::ADDED:
			added_files_size =
			    IcebergUtils::AddFileSizeChecked(added_files_size, manifest_entry.data_file.file_size_in_bytes);
			break;
		case IcebergManifestEntryStatusType::DELETED:
			deleted_files_size =
			    IcebergUtils::AddFileSizeChecked(deleted_files_size, manifest_entry.data_file.file_size_in_bytes);
			break;
		case IcebergManifestEntryStatusType::EXISTING:
			break;
		}
	}

	if (added_files_size > 0) {
		AddSizeMetric(IcebergSnapshotMetricType::ADDED_FILES_SIZE, added_files_size);
	}
	if (deleted_files_size > 0) {
		AddSizeMetric(IcebergSnapshotMetricType::REMOVED_FILES_SIZE, deleted_files_size);
	}
	UpdateTotalFilesSize(added_files_size, deleted_files_size);

	auto &manifest_file = manifest_list_entry.file;
	if (manifest_file.content == IcebergManifestContentType::DELETE) {
		//! Delete manifest (V2 position/equality delete files, V3 Puffin deletion vectors).
		//! Account for it under the Iceberg snapshot-summary delete metrics, mirroring the
		//! data-file accounting below — otherwise added DVs are silently invisible to any
		//! tooling that reads `total-delete-files` / `total-position-deletes` (e.g. compaction
		//! schedulers), and are erroneously counted as added data files.
		metrics.emplace(IcebergSnapshotMetricType::ADDED_DELETE_FILES, 0).first->second +=
		    manifest_file.added_files_count;
		metrics.emplace(IcebergSnapshotMetricType::REMOVED_DELETE_FILES, 0).first->second +=
		    manifest_file.deleted_files_count;
#ifndef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
		//! Without equality-delete writes every delete file is a positional delete file, so the
		//! added position-delete record count and the removed positional-delete-file count map
		//! directly from the manifest. Guarded so we don't mislabel equality deletes as positional
		//! once equality-delete writes are supported.
		metrics.emplace(IcebergSnapshotMetricType::ADDED_POSITION_DELETES, 0).first->second +=
		    manifest_file.added_rows_count;
		metrics.emplace(IcebergSnapshotMetricType::REMOVED_POSITION_DELETE_FILES, 0).first->second +=
		    manifest_file.deleted_files_count;
#endif

		auto previous_total_delete_files = metrics.find(IcebergSnapshotMetricType::TOTAL_DELETE_FILES);
		if (previous_total_delete_files != metrics.end()) {
			int64_t total_delete_files = previous_total_delete_files->second + manifest_file.added_files_count -
			                             manifest_file.deleted_files_count;
			if (total_delete_files >= 0) {
				metrics[IcebergSnapshotMetricType::TOTAL_DELETE_FILES] = total_delete_files;
			}
		}

#ifndef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
		auto previous_total_position_deletes = metrics.find(IcebergSnapshotMetricType::TOTAL_POSITION_DELETES);
		if (previous_total_position_deletes != metrics.end()) {
			int64_t total_position_deletes = previous_total_position_deletes->second + manifest_file.added_rows_count -
			                                 manifest_file.deleted_rows_count;
			if (total_position_deletes >= 0) {
				metrics[IcebergSnapshotMetricType::TOTAL_POSITION_DELETES] = total_position_deletes;
			}
		}
#endif
		return;
	}

	metrics.emplace(IcebergSnapshotMetricType::ADDED_DATA_FILES, 0).first->second += manifest_file.added_files_count;
	metrics.emplace(IcebergSnapshotMetricType::ADDED_RECORDS, 0).first->second += manifest_file.added_rows_count;
	metrics.emplace(IcebergSnapshotMetricType::DELETED_DATA_FILES, 0).first->second +=
	    manifest_file.deleted_files_count;
	metrics.emplace(IcebergSnapshotMetricType::DELETED_RECORDS, 0).first->second += manifest_file.deleted_rows_count;

	auto previous_total_files = metrics.find(IcebergSnapshotMetricType::TOTAL_DATA_FILES);
	if (previous_total_files != metrics.end()) {
		int64_t total_files =
		    previous_total_files->second + manifest_file.added_files_count - manifest_file.deleted_files_count;
		if (total_files >= 0) {
			metrics[IcebergSnapshotMetricType::TOTAL_DATA_FILES] = total_files;
		}
	}

	auto previous_total_records = metrics.find(IcebergSnapshotMetricType::TOTAL_RECORDS);
	if (previous_total_records != metrics.end()) {
		int64_t total_records =
		    previous_total_records->second + manifest_file.added_rows_count - manifest_file.deleted_rows_count;
		if (total_records >= 0) {
			metrics[IcebergSnapshotMetricType::TOTAL_RECORDS] = total_records;
		}
	}
}

void IcebergSnapshotMetrics::RemoveFileSize(int64_t file_size_in_bytes) {
	AddSizeMetric(IcebergSnapshotMetricType::REMOVED_FILES_SIZE, file_size_in_bytes);
	UpdateTotalFilesSize(0, file_size_in_bytes);
}

bool IcebergSnapshotMetrics::HasTotalFilesSize() const {
	return metrics.count(IcebergSnapshotMetricType::TOTAL_FILES_SIZE) != 0;
}

void IcebergSnapshotMetrics::SetTotalFilesSize(int64_t total_files_size) {
	if (total_files_size < 0) {
		throw InvalidConfigurationException("Iceberg snapshot 'total-files-size' cannot be negative");
	}
	metrics[IcebergSnapshotMetricType::TOTAL_FILES_SIZE] = total_files_size;
}

static string OperationTypeToString(IcebergSnapshotOperationType type) {
	switch (type) {
	case IcebergSnapshotOperationType::APPEND:
		return "append";
	case IcebergSnapshotOperationType::REPLACE:
		return "replace";
	case IcebergSnapshotOperationType::OVERWRITE:
		return "overwrite";
	case IcebergSnapshotOperationType::DELETE:
		return "delete";
	default:
		throw InvalidConfigurationException("Operation type not implemented: %d", static_cast<uint8_t>(type));
	}
}

namespace {

struct IcebergSnapshotMetricItem {
	IcebergSnapshotMetricType type;
	const char *name;
};

static const IcebergSnapshotMetricItem SNAPSHOT_METRIC_KEYS[] = {
    {IcebergSnapshotMetricType::ADDED_DATA_FILES, "added-data-files"},
    {IcebergSnapshotMetricType::ADDED_RECORDS, "added-records"},
    {IcebergSnapshotMetricType::DELETED_DATA_FILES, "deleted-data-files"},
    {IcebergSnapshotMetricType::DELETED_RECORDS, "deleted-records"},
    {IcebergSnapshotMetricType::ADDED_DELETE_FILES, "added-delete-files"},
    {IcebergSnapshotMetricType::ADDED_POSITION_DELETES, "added-position-deletes"},
    {IcebergSnapshotMetricType::REMOVED_DELETE_FILES, "removed-delete-files"},
    {IcebergSnapshotMetricType::REMOVED_POSITION_DELETE_FILES, "removed-position-delete-files"},
    {IcebergSnapshotMetricType::ADDED_FILES_SIZE, "added-files-size"},
    {IcebergSnapshotMetricType::REMOVED_FILES_SIZE, "removed-files-size"},
    {IcebergSnapshotMetricType::TOTAL_DATA_FILES, "total-data-files"},
    {IcebergSnapshotMetricType::TOTAL_RECORDS, "total-records"},
    {IcebergSnapshotMetricType::TOTAL_DELETE_FILES, "total-delete-files"},
    {IcebergSnapshotMetricType::TOTAL_POSITION_DELETES, "total-position-deletes"},
    {IcebergSnapshotMetricType::TOTAL_FILES_SIZE, "total-files-size"}};

static const idx_t SNAPSHOT_METRIC_KEYS_SIZE = sizeof(SNAPSHOT_METRIC_KEYS) / sizeof(IcebergSnapshotMetricItem);

} // namespace

static string MetricsTypeToString(IcebergSnapshotMetricType type) {
	for (idx_t i = 0; i < SNAPSHOT_METRIC_KEYS_SIZE; i++) {
		auto &item = SNAPSHOT_METRIC_KEYS[i];
		if (item.type == type) {
			return item.name;
		}
	}
	throw InvalidConfigurationException("Metrics type not implemented: %d", static_cast<uint8_t>(type));
}

static IcebergSnapshotMetrics MetricsFromSummary(const case_insensitive_map_t<string> &snapshot_summary) {
	IcebergSnapshotMetrics ret;
	auto &metrics = ret.metrics;
	//! Remove the default for `TOTAL_FILES_SIZE`, leaving it uninitialized if not present in the summary, rather than
	//! setting it to 0.
	metrics.erase(IcebergSnapshotMetricType::TOTAL_FILES_SIZE);
	bool file_size_metrics_are_valid = true;
	for (idx_t i = 0; i < SNAPSHOT_METRIC_KEYS_SIZE; i++) {
		auto &item = SNAPSHOT_METRIC_KEYS[i];
		const bool is_file_size_metric = item.type == IcebergSnapshotMetricType::ADDED_FILES_SIZE ||
		                                 item.type == IcebergSnapshotMetricType::REMOVED_FILES_SIZE ||
		                                 item.type == IcebergSnapshotMetricType::TOTAL_FILES_SIZE;
		auto it = snapshot_summary.find(item.name);
		if (it != snapshot_summary.end()) {
			int64_t value;
			try {
				value = std::stoll(it->second);
			} catch (...) {
				if (is_file_size_metric) {
					file_size_metrics_are_valid = false;
				}
				// Skip invalid metrics
				continue;
			}
			if (is_file_size_metric && value < 0) {
				file_size_metrics_are_valid = false;
				continue;
			}
			metrics[item.type] = value;
		}
	}
	if (!file_size_metrics_are_valid) {
		metrics.erase(IcebergSnapshotMetricType::ADDED_FILES_SIZE);
		metrics.erase(IcebergSnapshotMetricType::REMOVED_FILES_SIZE);
		metrics.erase(IcebergSnapshotMetricType::TOTAL_FILES_SIZE);
	}
	return ret;
}

rest_api_objects::Snapshot IcebergSnapshot::ToRESTObject(const IcebergTableMetadata &table_metadata) const {
	rest_api_objects::Snapshot res;

	if (!snapshot_id) {
		throw InvalidConfigurationException("snapshot.snapshot_id is not set");
	}
	res.snapshot_id = *snapshot_id;
	res.timestamp_ms = timestamp_ms.value;
	res.manifest_list = manifest_list;

	res.summary.operation = OperationTypeToString(operation);
	auto &metrics_map = metrics.metrics;
	for (auto &entry : metrics_map) {
		res.summary.additional_properties[MetricsTypeToString(entry.first)] = std::to_string(entry.second);
	}

	if (parent_snapshot_id) {
		res.parent_snapshot_id = *parent_snapshot_id;
	}

	if (added_rows) {
		res.added_rows = *added_rows;
	}

	res.sequence_number = sequence_number;

	res.schema_id = schema_id;

	if (first_row_id) {
		res.first_row_id = *first_row_id;
	} else if (table_metadata.iceberg_version >= 3) {
		throw InternalException("first-row-id required for V3 tables!");
	}

	return res;
}

IcebergSnapshot IcebergSnapshot::ParseSnapshot(const rest_api_objects::Snapshot &snapshot,
                                               IcebergTableMetadata &metadata) {
	if (!snapshot.schema_id) {
		throw InvalidConfigurationException("snapshot.schema_id is not set");
	}

	IcebergSnapshot ret(*snapshot.schema_id);
	if (metadata.iceberg_version == 1) {
		//! SPEC: Snapshot field sequence-number must default to 0
		ret.sequence_number = 0;
	} else if (metadata.iceberg_version >= 2) {
		D_ASSERT(snapshot.sequence_number);
		ret.sequence_number = *snapshot.sequence_number;
	}

	ret.snapshot_id = snapshot.snapshot_id;
	ret.timestamp_ms = timestamp_ms_t(snapshot.timestamp_ms);
	ret.manifest_list = snapshot.manifest_list;
	ret.metrics = MetricsFromSummary(snapshot.summary.additional_properties);

	auto &op = snapshot.summary.operation;
	if (op == "append") {
		ret.operation = IcebergSnapshotOperationType::APPEND;
	} else if (op == "replace") {
		ret.operation = IcebergSnapshotOperationType::REPLACE;
	} else if (op == "overwrite") {
		ret.operation = IcebergSnapshotOperationType::OVERWRITE;
	} else if (op == "delete") {
		ret.operation = IcebergSnapshotOperationType::DELETE;
	} else {
		throw InvalidConfigurationException("Unknown snapshot operation type: '%s'", op);
	}

	if (snapshot.first_row_id) {
		ret.first_row_id = *snapshot.first_row_id;
	}

	if (snapshot.added_rows) {
		ret.added_rows = *snapshot.added_rows;
	}
	return ret;
}

int32_t IcebergSnapshot::GetSchemaId() const {
	return schema_id;
}

} // namespace duckdb
