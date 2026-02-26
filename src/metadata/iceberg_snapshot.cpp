#include "metadata/iceberg_snapshot.hpp"
#include "metadata/iceberg_table_metadata.hpp"
#include "storage/iceberg_table_information.hpp"

namespace duckdb {

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

struct SnapshotMetricItem {
	SnapshotMetricType type;
	const char *name;
};

static const SnapshotMetricItem SNAPSHOT_METRIC_KEYS[] = {
    {SnapshotMetricType::ADDED_DATA_FILES, "added-data-files"},
    {SnapshotMetricType::ADDED_RECORDS, "added-records"},
    {SnapshotMetricType::DELETED_DATA_FILES, "deleted-data-files"},
    {SnapshotMetricType::DELETED_RECORDS, "deleted-records"},
    {SnapshotMetricType::TOTAL_DATA_FILES, "total-data-files"},
    {SnapshotMetricType::TOTAL_RECORDS, "total-records"}};

static const idx_t SNAPSHOT_METRIC_KEYS_SIZE = sizeof(SNAPSHOT_METRIC_KEYS) / sizeof(SnapshotMetricItem);

} // namespace

static string MetricsTypeToString(SnapshotMetricType type) {
	for (idx_t i = 0; i < SNAPSHOT_METRIC_KEYS_SIZE; i++) {
		auto &item = SNAPSHOT_METRIC_KEYS[i];
		if (item.type == type) {
			return item.name;
		}
	}
	throw InvalidConfigurationException("Metrics type not implemented: %d", static_cast<uint8_t>(type));
}

static IcebergSnapshot::metrics_map_t MetricsFromSummary(const case_insensitive_map_t<string> &snapshot_summary) {
	IcebergSnapshot::metrics_map_t metrics;
	for (idx_t i = 0; i < SNAPSHOT_METRIC_KEYS_SIZE; i++) {
		auto &item = SNAPSHOT_METRIC_KEYS[i];
		auto it = snapshot_summary.find(item.name);
		if (it != snapshot_summary.end()) {
			int64_t value;
			try {
				value = std::stoll(it->second);
			} catch (...) {
				// Skip invalid metrics
				continue;
			}
			metrics[item.type] = value;
		}
	}
	return metrics;
}

rest_api_objects::Snapshot IcebergSnapshot::ToRESTObject(const IcebergTableInformation &table_info) const {
	rest_api_objects::Snapshot res;

	res.snapshot_id = snapshot_id;
	res.timestamp_ms = timestamp_ms.value;
	res.manifest_list = manifest_list;

	res.summary.operation = OperationTypeToString(operation);
	for (auto &entry : metrics) {
		res.summary.additional_properties[MetricsTypeToString(entry.first)] = std::to_string(entry.second);
	}

	if (!has_parent_snapshot) {
		res.has_parent_snapshot_id = false;
	} else {
		res.has_parent_snapshot_id = true;
		res.parent_snapshot_id = parent_snapshot_id;
	}

	if (has_added_rows) {
		res.has_added_rows = true;
		res.added_rows = added_rows;
	}

	res.has_sequence_number = true;
	res.sequence_number = sequence_number;

	res.has_schema_id = true;
	res.schema_id = schema_id;

	if (has_first_row_id) {
		res.has_first_row_id = true;
		res.first_row_id = first_row_id;
	} else if (table_info.GetIcebergVersion() >= 3) {
		throw InternalException("first-row-id required for V3 tables!");
	}

	return res;
}

IcebergSnapshot IcebergSnapshot::ParseSnapshot(const rest_api_objects::Snapshot &snapshot,
                                               IcebergTableMetadata &metadata) {
	IcebergSnapshot ret;
	if (metadata.iceberg_version == 1) {
		ret.sequence_number = 0;
	} else if (metadata.iceberg_version >= 2) {
		D_ASSERT(snapshot.has_sequence_number);
		ret.sequence_number = snapshot.sequence_number;
	}

	ret.snapshot_id = snapshot.snapshot_id;
	ret.timestamp_ms = Timestamp::FromEpochMs(snapshot.timestamp_ms);
	D_ASSERT(snapshot.has_schema_id);
	ret.schema_id = snapshot.schema_id;
	ret.manifest_list = snapshot.manifest_list;
	ret.metrics = MetricsFromSummary(snapshot.summary.additional_properties);

	ret.has_first_row_id = snapshot.has_first_row_id;
	ret.first_row_id = snapshot.first_row_id;

	ret.has_added_rows = snapshot.has_added_rows;
	ret.added_rows = snapshot.added_rows;
	return ret;
}

} // namespace duckdb
