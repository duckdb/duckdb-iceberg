#include "metadata/iceberg_snapshot.hpp"
#include "metadata/iceberg_table_metadata.hpp"
#include "storage/iceberg_table_information.hpp"
#include "duckdb/common/types/uuid.hpp"

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
	return map<IcebergSnapshotMetricType, int64_t>(
	    {{IcebergSnapshotMetricType::TOTAL_DATA_FILES, 0}, {IcebergSnapshotMetricType::TOTAL_RECORDS, 0}});
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
}

void IcebergSnapshotMetrics::AddManifestFile(const IcebergManifestFile &manifest_file) {
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
    {IcebergSnapshotMetricType::TOTAL_DATA_FILES, "total-data-files"},
    {IcebergSnapshotMetricType::TOTAL_RECORDS, "total-records"}};

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
	return ret;
}

rest_api_objects::Snapshot IcebergSnapshot::ToRESTObject(const IcebergTableMetadata &table_metadata) const {
	rest_api_objects::Snapshot res;

	res.snapshot_id = snapshot_id;
	res.timestamp_ms = timestamp_ms.value;
	res.manifest_list = manifest_list;

	res.summary.operation = OperationTypeToString(operation);
	auto &metrics_map = metrics.metrics;
	for (auto &entry : metrics_map) {
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
	} else if (table_metadata.iceberg_version >= 3) {
		throw InternalException("first-row-id required for V3 tables!");
	}

	return res;
}

IcebergSnapshot IcebergSnapshot::ParseSnapshot(const rest_api_objects::Snapshot &snapshot,
                                               IcebergTableMetadata &metadata) {
	IcebergSnapshot ret;
	if (metadata.iceberg_version == 1) {
		//! SPEC: Snapshot field sequence-number must default to 0
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

yyjson_mut_val *IcebergSnapshot::ToJSON(const rest_api_objects::Snapshot &snapshot, yyjson_mut_doc *doc) {
	auto snapshot_obj = yyjson_mut_obj(doc);

	yyjson_mut_obj_add_uint(doc, snapshot_obj, "snapshot-id", snapshot.snapshot_id);
	if (snapshot.has_parent_snapshot_id) {
		yyjson_mut_obj_add_uint(doc, snapshot_obj, "parent-snapshot-id", snapshot.parent_snapshot_id);
	}
	yyjson_mut_obj_add_uint(doc, snapshot_obj, "sequence-number", snapshot.sequence_number);
	yyjson_mut_obj_add_uint(doc, snapshot_obj, "timestamp-ms", snapshot.timestamp_ms);
	yyjson_mut_obj_add_strcpy(doc, snapshot_obj, "manifest-list", snapshot.manifest_list.c_str());
	auto summary_json = yyjson_mut_obj_add_obj(doc, snapshot_obj, "summary");
	yyjson_mut_obj_add_strcpy(doc, summary_json, "operation", snapshot.summary.operation.c_str());
	for (auto &prop : snapshot.summary.additional_properties) {
		//! Register the string as part of the document, to ensure lifetime correctness
		auto &key = prop.first;
		auto key_val = unsafe_yyjson_mut_strncpy(doc, key.c_str(), key.size());
		yyjson_mut_obj_add_strcpy(doc, summary_json, key_val, prop.second.c_str());
	}
	yyjson_mut_obj_add_uint(doc, snapshot_obj, "schema-id", snapshot.schema_id);
	if (snapshot.has_first_row_id) {
		yyjson_mut_obj_add_uint(doc, snapshot_obj, "first-row-id", snapshot.first_row_id);
	}
	if (snapshot.has_added_rows) {
		yyjson_mut_obj_add_uint(doc, snapshot_obj, "added-rows", snapshot.added_rows);
	}
	return snapshot_obj;
}

} // namespace duckdb
