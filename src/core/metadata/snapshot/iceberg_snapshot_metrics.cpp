#include "core/metadata/snapshot/iceberg_snapshot_metrics.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "common/iceberg_utils.hpp"

namespace duckdb {

namespace {

struct IcebergSnapshotMetricItem {
	IcebergSnapshotMetricType type;
	const char *name;
};

static const IcebergSnapshotMetricItem SNAPSHOT_METRIC_KEYS[] = {
    {IcebergSnapshotMetricType::ADDED_DATA_FILES, "added-data-files"},
    {IcebergSnapshotMetricType::DELETED_DATA_FILES, "deleted-data-files"},
    {IcebergSnapshotMetricType::TOTAL_DATA_FILES, "total-data-files"},
    {IcebergSnapshotMetricType::ADDED_DELETE_FILES, "added-delete-files"},
    {IcebergSnapshotMetricType::ADDED_EQUALITY_DELETE_FILES, "added-equality-delete-files"},
    {IcebergSnapshotMetricType::REMOVED_EQUALITY_DELETE_FILES, "removed-equality-delete-files"},
    {IcebergSnapshotMetricType::ADDED_POSITION_DELETE_FILES, "added-position-delete-files"},
    {IcebergSnapshotMetricType::REMOVED_POSITION_DELETE_FILES, "removed-position-delete-files"},
    {IcebergSnapshotMetricType::ADDED_DVS, "added-dvs"},
    {IcebergSnapshotMetricType::REMOVED_DVS, "removed-dvs"},
    {IcebergSnapshotMetricType::REMOVED_DELETE_FILES, "removed-delete-files"},
    {IcebergSnapshotMetricType::TOTAL_DELETE_FILES, "total-delete-files"},
    {IcebergSnapshotMetricType::ADDED_RECORDS, "added-records"},
    {IcebergSnapshotMetricType::DELETED_RECORDS, "deleted-records"},
    {IcebergSnapshotMetricType::TOTAL_RECORDS, "total-records"},
    {IcebergSnapshotMetricType::ADDED_FILES_SIZE, "added-files-size"},
    {IcebergSnapshotMetricType::REMOVED_FILES_SIZE, "removed-files-size"},
    {IcebergSnapshotMetricType::TOTAL_FILES_SIZE, "total-files-size"},
    {IcebergSnapshotMetricType::ADDED_POSITION_DELETES, "added-position-deletes"},
    {IcebergSnapshotMetricType::REMOVED_POSITION_DELETES, "removed-position-deletes"},
    {IcebergSnapshotMetricType::TOTAL_POSITION_DELETES, "total-position-deletes"},
    {IcebergSnapshotMetricType::ADDED_EQUALITY_DELETES, "added-equality-deletes"},
    {IcebergSnapshotMetricType::REMOVED_EQUALITY_DELETES, "removed-equality-deletes"},
    {IcebergSnapshotMetricType::TOTAL_EQUALITY_DELETES, "total-equality-deletes"},
    {IcebergSnapshotMetricType::DELETED_DUPLICATE_FILES, "deleted-duplicate-files"},
    {IcebergSnapshotMetricType::CHANGED_PARTITION_COUNT, "changed-partition-count"},
    {IcebergSnapshotMetricType::MANIFESTS_CREATED, "manifests-created"},
    {IcebergSnapshotMetricType::MANIFESTS_KEPT, "manifests-kept"},
    {IcebergSnapshotMetricType::MANIFESTS_REPLACED, "manifests-replaced"},
    {IcebergSnapshotMetricType::ENTRIES_PROCESSED, "entries-processed"},
};

static const idx_t SNAPSHOT_METRIC_KEYS_SIZE = sizeof(SNAPSHOT_METRIC_KEYS) / sizeof(IcebergSnapshotMetricItem);

static string MetricsTypeToString(IcebergSnapshotMetricType type) {
	for (idx_t i = 0; i < SNAPSHOT_METRIC_KEYS_SIZE; i++) {
		auto &item = SNAPSHOT_METRIC_KEYS[i];
		if (item.type == type) {
			return item.name;
		}
	}
	throw InvalidConfigurationException("Metrics type not implemented: %d", static_cast<uint8_t>(type));
}

static bool TryParseMetricValue(const string &raw_value, int64_t &value) {
	if (raw_value.empty()) {
		return false;
	}
	for (auto character : raw_value) {
		if (character < '0' || character > '9') {
			return false;
		}
	}
	try {
		size_t parsed_characters = 0;
		value = std::stoll(raw_value, &parsed_characters);
		return parsed_characters == raw_value.size();
	} catch (...) {
		return false;
	}
}

struct ManifestMetrics {
	int64_t data_files = 0;
	int64_t records = 0;
	int64_t delete_files = 0;
	int64_t position_delete_files = 0;
	int64_t equality_delete_files = 0;
	int64_t position_deletes = 0;
	int64_t equality_deletes = 0;
	int64_t deletion_vectors = 0;
	int64_t files_size = 0;
};

struct ManifestChanges {
	ManifestMetrics added;
	ManifestMetrics removed;
};

static int64_t AddManifestRecords(int64_t current, int64_t count) {
	int64_t result;
	if (count < 0 || !TryAddOperator::Operation(current, count, result)) {
		throw InvalidConfigurationException("Iceberg manifest record counts must fit in a nonnegative BIGINT");
	}
	return result;
}

static ManifestChanges CollectManifestChanges(IcebergManifestContentType content,
                                              const vector<IcebergManifestEntry> &entries) {
	ManifestChanges changes;
	for (const auto &entry : entries) {
		if (entry.status == IcebergManifestEntryStatusType::EXISTING) {
			continue;
		}
		if (entry.status != IcebergManifestEntryStatusType::ADDED &&
		    entry.status != IcebergManifestEntryStatusType::DELETED) {
			throw InvalidConfigurationException("Invalid manifest entry status");
		}
		auto &metrics = entry.status == IcebergManifestEntryStatusType::ADDED ? changes.added : changes.removed;
		const auto &file = entry.data_file;
		if ((content == IcebergManifestContentType::DATA) != (file.content == IcebergManifestEntryContentType::DATA)) {
			throw InvalidConfigurationException("Data file content does not match its manifest content");
		}
		metrics.files_size = IcebergUtils::AddFileSizeChecked(metrics.files_size, file.GetContentSizeInBytes());
		switch (file.content) {
		case IcebergManifestEntryContentType::DATA:
			metrics.data_files++;
			metrics.records = AddManifestRecords(metrics.records, file.record_count);
			break;
		case IcebergManifestEntryContentType::POSITION_DELETES:
			metrics.delete_files++;
			metrics.position_deletes = AddManifestRecords(metrics.position_deletes, file.record_count);
			if (file.IsDeletionVector()) {
				metrics.deletion_vectors++;
			} else {
				metrics.position_delete_files++;
			}
			break;
		case IcebergManifestEntryContentType::EQUALITY_DELETES:
			metrics.delete_files++;
			metrics.equality_delete_files++;
			metrics.equality_deletes = AddManifestRecords(metrics.equality_deletes, file.record_count);
			break;
		default:
			throw InvalidConfigurationException("Unsupported Iceberg manifest entry content type");
		}
	}
	return changes;
}

} // namespace

metrics_map_t IcebergSnapshotMetrics::EmptyMetrics() {
	//! First snapshot of a table, start all totals at 0
	return metrics_map_t({{IcebergSnapshotMetricType::TOTAL_DATA_FILES, 0},
	                      {IcebergSnapshotMetricType::TOTAL_DELETE_FILES, 0},
	                      {IcebergSnapshotMetricType::TOTAL_RECORDS, 0},
	                      {IcebergSnapshotMetricType::TOTAL_FILES_SIZE, 0},
	                      {IcebergSnapshotMetricType::TOTAL_POSITION_DELETES, 0},
	                      {IcebergSnapshotMetricType::TOTAL_EQUALITY_DELETES, 0}});
}

IcebergSnapshotMetrics::IcebergSnapshotMetrics() : metrics(EmptyMetrics()) {
}

void IcebergSnapshotMetrics::InheritMetric(const IcebergSnapshotMetrics &parent, IcebergSnapshotMetricType metric) {
	auto it = parent.metrics.find(metric);
	if (it == parent.metrics.end()) {
		//! Nothing to inherit
		return;
	}
	metrics[metric] = it->second;
}

IcebergSnapshotMetrics::IcebergSnapshotMetrics(const IcebergSnapshot &parent_snapshot) {
	//! Start metrics from a parent snapshot, inherit all totals
	InheritMetric(parent_snapshot.metrics, IcebergSnapshotMetricType::TOTAL_DATA_FILES);
	InheritMetric(parent_snapshot.metrics, IcebergSnapshotMetricType::TOTAL_DELETE_FILES);
	InheritMetric(parent_snapshot.metrics, IcebergSnapshotMetricType::TOTAL_RECORDS);
	InheritMetric(parent_snapshot.metrics, IcebergSnapshotMetricType::TOTAL_FILES_SIZE);
	InheritMetric(parent_snapshot.metrics, IcebergSnapshotMetricType::TOTAL_POSITION_DELETES);
	InheritMetric(parent_snapshot.metrics, IcebergSnapshotMetricType::TOTAL_EQUALITY_DELETES);
}

IcebergSnapshotMetrics::IcebergSnapshotMetrics(const case_insensitive_map_t<string> &snapshot_summary) {
	bool file_size_metrics_are_valid = true;
	for (idx_t i = 0; i < SNAPSHOT_METRIC_KEYS_SIZE; i++) {
		auto &item = SNAPSHOT_METRIC_KEYS[i];
		const bool is_file_size_metric = item.type == IcebergSnapshotMetricType::ADDED_FILES_SIZE ||
		                                 item.type == IcebergSnapshotMetricType::REMOVED_FILES_SIZE ||
		                                 item.type == IcebergSnapshotMetricType::TOTAL_FILES_SIZE;
		auto it = snapshot_summary.find(item.name);
		if (it == snapshot_summary.end()) {
			//! Not present in the summary
			continue;
		}
		int64_t value;
		if (!TryParseMetricValue(it->second, value)) {
			if (is_file_size_metric) {
				file_size_metrics_are_valid = false;
			}
			// Skip invalid metrics
			continue;
		}
		metrics[item.type] = value;
	}
	if (!file_size_metrics_are_valid) {
		metrics.erase(IcebergSnapshotMetricType::ADDED_FILES_SIZE);
		metrics.erase(IcebergSnapshotMetricType::REMOVED_FILES_SIZE);
		metrics.erase(IcebergSnapshotMetricType::TOTAL_FILES_SIZE);
	}
}

void IcebergSnapshotMetrics::AddSizeMetric(IcebergSnapshotMetricType type, int64_t value) {
	auto &metric = metrics.emplace(type, 0).first->second;
	metric = IcebergUtils::AddFileSizeChecked(metric, value);
}

void IcebergSnapshotMetrics::AddMetric(IcebergSnapshotMetricType type, int64_t value) {
	if (value < 0) {
		throw InvalidConfigurationException("Iceberg snapshot metric '%s' cannot be negative",
		                                    MetricsTypeToString(type));
	}
	auto &metric = metrics.emplace(type, 0).first->second;
	int64_t updated;
	if (!TryAddOperator::Operation(metric, value, updated)) {
		throw InvalidConfigurationException("Iceberg snapshot metric '%s' exceeds the supported BIGINT range",
		                                    MetricsTypeToString(type));
	}
	metric = updated;
}

void IcebergSnapshotMetrics::UpdateTotalMetric(IcebergSnapshotMetricType type, int64_t added, int64_t removed) {
	if (added < 0 || removed < 0) {
		throw InvalidConfigurationException("Cannot update Iceberg snapshot metric '%s' with negative values",
		                                    MetricsTypeToString(type));
	}
	auto total_it = metrics.find(type);
	if (total_it == metrics.end()) {
		return;
	}
	int64_t with_added;
	int64_t updated;
	if (!TryAddOperator::Operation(total_it->second, added, with_added) ||
	    !TrySubtractOperator::Operation(with_added, removed, updated)) {
		throw InvalidConfigurationException("Iceberg snapshot metric '%s' exceeds the supported BIGINT range",
		                                    MetricsTypeToString(type));
	}
	if (updated < 0) {
		//! An inherited optional total is inconsistent with the files being removed. Keep it unknown rather than
		//! emitting an incorrect value or rejecting an otherwise valid commit.
		metrics.erase(total_it);
		return;
	}
	total_it->second = updated;
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

void IcebergSnapshotMetrics::AddManifestEntries(IcebergManifestContentType content,
                                                const vector<IcebergManifestEntry> &entries) {
	auto changes = CollectManifestChanges(content, entries);
	const auto &added = changes.added;
	const auto &removed = changes.removed;

	if (added.files_size > 0) {
		AddSizeMetric(IcebergSnapshotMetricType::ADDED_FILES_SIZE, added.files_size);
	}
	if (removed.files_size > 0) {
		AddSizeMetric(IcebergSnapshotMetricType::REMOVED_FILES_SIZE, removed.files_size);
	}
	UpdateTotalFilesSize(added.files_size, removed.files_size);

	if (content == IcebergManifestContentType::DELETE) {
		//! Delete file count metrics
		AddMetric(IcebergSnapshotMetricType::ADDED_DELETE_FILES, added.delete_files);
		AddMetric(IcebergSnapshotMetricType::REMOVED_DELETE_FILES, removed.delete_files);
		UpdateTotalMetric(IcebergSnapshotMetricType::TOTAL_DELETE_FILES, added.delete_files, removed.delete_files);

		//! Position delete file and record metrics. Deletion vectors contribute records, but not position delete files.
		AddMetric(IcebergSnapshotMetricType::ADDED_POSITION_DELETE_FILES, added.position_delete_files);
		AddMetric(IcebergSnapshotMetricType::REMOVED_POSITION_DELETE_FILES, removed.position_delete_files);
		AddMetric(IcebergSnapshotMetricType::ADDED_DVS, added.deletion_vectors);
		AddMetric(IcebergSnapshotMetricType::REMOVED_DVS, removed.deletion_vectors);
		AddMetric(IcebergSnapshotMetricType::ADDED_POSITION_DELETES, added.position_deletes);
		AddMetric(IcebergSnapshotMetricType::REMOVED_POSITION_DELETES, removed.position_deletes);
		UpdateTotalMetric(IcebergSnapshotMetricType::TOTAL_POSITION_DELETES, added.position_deletes,
		                  removed.position_deletes);

		//! Equality delete file and record metrics
		AddMetric(IcebergSnapshotMetricType::ADDED_EQUALITY_DELETE_FILES, added.equality_delete_files);
		AddMetric(IcebergSnapshotMetricType::REMOVED_EQUALITY_DELETE_FILES, removed.equality_delete_files);
		AddMetric(IcebergSnapshotMetricType::ADDED_EQUALITY_DELETES, added.equality_deletes);
		AddMetric(IcebergSnapshotMetricType::REMOVED_EQUALITY_DELETES, removed.equality_deletes);
		UpdateTotalMetric(IcebergSnapshotMetricType::TOTAL_EQUALITY_DELETES, added.equality_deletes,
		                  removed.equality_deletes);
		return;
	}

	//! Data file count metrics
	AddMetric(IcebergSnapshotMetricType::ADDED_DATA_FILES, added.data_files);
	AddMetric(IcebergSnapshotMetricType::DELETED_DATA_FILES, removed.data_files);
	UpdateTotalMetric(IcebergSnapshotMetricType::TOTAL_DATA_FILES, added.data_files, removed.data_files);

	//! Data record metrics
	AddMetric(IcebergSnapshotMetricType::ADDED_RECORDS, added.records);
	AddMetric(IcebergSnapshotMetricType::DELETED_RECORDS, removed.records);
	UpdateTotalMetric(IcebergSnapshotMetricType::TOTAL_RECORDS, added.records, removed.records);
}

void IcebergSnapshotMetrics::RemoveManifestEntry(const IcebergManifestEntry &manifest_entry) {
	auto &data_file = manifest_entry.data_file;
	auto content_size = data_file.GetContentSizeInBytes();
	AddSizeMetric(IcebergSnapshotMetricType::REMOVED_FILES_SIZE, content_size);
	UpdateTotalFilesSize(0, content_size);

	switch (data_file.content) {
	case IcebergManifestEntryContentType::DATA:
		AddMetric(IcebergSnapshotMetricType::DELETED_DATA_FILES, 1);
		AddMetric(IcebergSnapshotMetricType::DELETED_RECORDS, data_file.record_count);
		UpdateTotalMetric(IcebergSnapshotMetricType::TOTAL_DATA_FILES, 0, 1);
		UpdateTotalMetric(IcebergSnapshotMetricType::TOTAL_RECORDS, 0, data_file.record_count);
		break;
	case IcebergManifestEntryContentType::POSITION_DELETES:
		AddMetric(IcebergSnapshotMetricType::REMOVED_DELETE_FILES, 1);
		UpdateTotalMetric(IcebergSnapshotMetricType::TOTAL_DELETE_FILES, 0, 1);
		if (data_file.IsDeletionVector()) {
			AddMetric(IcebergSnapshotMetricType::REMOVED_DVS, 1);
		} else {
			AddMetric(IcebergSnapshotMetricType::REMOVED_POSITION_DELETE_FILES, 1);
		}
		AddMetric(IcebergSnapshotMetricType::REMOVED_POSITION_DELETES, data_file.record_count);
		UpdateTotalMetric(IcebergSnapshotMetricType::TOTAL_POSITION_DELETES, 0, data_file.record_count);
		break;
	case IcebergManifestEntryContentType::EQUALITY_DELETES:
		AddMetric(IcebergSnapshotMetricType::REMOVED_DELETE_FILES, 1);
		AddMetric(IcebergSnapshotMetricType::REMOVED_EQUALITY_DELETE_FILES, 1);
		AddMetric(IcebergSnapshotMetricType::REMOVED_EQUALITY_DELETES, data_file.record_count);
		UpdateTotalMetric(IcebergSnapshotMetricType::TOTAL_DELETE_FILES, 0, 1);
		UpdateTotalMetric(IcebergSnapshotMetricType::TOTAL_EQUALITY_DELETES, 0, data_file.record_count);
		break;
	default:
		throw InvalidConfigurationException("Unsupported Iceberg manifest entry content type");
	}
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

const metrics_map_t &IcebergSnapshotMetrics::GetMetrics() const {
	return metrics;
}

case_insensitive_map_t<string> IcebergSnapshotMetrics::ToString() const {
	case_insensitive_map_t<string> result;
	for (auto &entry : metrics) {
		result[MetricsTypeToString(entry.first)] = std::to_string(entry.second);
	}
	return result;
}

} // namespace duckdb
