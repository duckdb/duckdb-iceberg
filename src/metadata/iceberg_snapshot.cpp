#include "metadata/iceberg_snapshot.hpp"
#include "metadata/iceberg_table_metadata.hpp"

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

rest_api_objects::Snapshot IcebergSnapshot::ToRESTObject() const {
	rest_api_objects::Snapshot res;

	res.snapshot_id = snapshot_id;
	res.timestamp_ms = timestamp_ms.value;
	res.manifest_list = manifest_list;

	res.summary.operation = OperationTypeToString(operation);

	//! Add summary statistics to additional_properties if available
	if (summary.has_statistics) {
		auto &props = res.summary.additional_properties;

		//! Cumulative totals (required by Redshift and other engines)
		props["total-records"] = std::to_string(summary.total_records);
		props["total-data-files"] = std::to_string(summary.total_data_files);
		props["total-files-size"] = std::to_string(summary.total_files_size);
		props["total-delete-files"] = std::to_string(summary.total_delete_files);
		props["total-position-deletes"] = std::to_string(summary.total_position_deletes);
		props["total-equality-deletes"] = std::to_string(summary.total_equality_deletes);

		//! Delta values for this snapshot
		if (summary.added_records > 0 || operation == IcebergSnapshotOperationType::APPEND) {
			props["added-records"] = std::to_string(summary.added_records);
		}
		if (summary.added_data_files > 0 || operation == IcebergSnapshotOperationType::APPEND) {
			props["added-data-files"] = std::to_string(summary.added_data_files);
		}
		if (summary.added_files_size > 0 || operation == IcebergSnapshotOperationType::APPEND) {
			props["added-files-size"] = std::to_string(summary.added_files_size);
		}
		if (summary.deleted_records > 0) {
			props["deleted-records"] = std::to_string(summary.deleted_records);
		}
		if (summary.deleted_data_files > 0) {
			props["deleted-data-files"] = std::to_string(summary.deleted_data_files);
		}
		if (summary.removed_files_size > 0) {
			props["removed-files-size"] = std::to_string(summary.removed_files_size);
		}
		if (summary.changed_partition_count > 0) {
			props["changed-partition-count"] = std::to_string(summary.changed_partition_count);
		}
	}

	if (!has_parent_snapshot) {
		res.has_parent_snapshot_id = false;
	} else {
		res.has_parent_snapshot_id = true;
		res.parent_snapshot_id = parent_snapshot_id;
	}

	res.has_sequence_number = true;
	res.sequence_number = sequence_number;

	res.has_schema_id = true;
	res.schema_id = schema_id;

	return res;
}

static int64_t ParseInt64FromProps(const case_insensitive_map_t<string> &props, const string &key, int64_t default_val = 0) {
	auto it = props.find(key);
	if (it != props.end()) {
		try {
			return std::stoll(it->second);
		} catch (...) {
			return default_val;
		}
	}
	return default_val;
}

IcebergSnapshot IcebergSnapshot::ParseSnapshot(rest_api_objects::Snapshot &snapshot, IcebergTableMetadata &metadata) {
	IcebergSnapshot ret;
	if (metadata.iceberg_version == 1) {
		ret.sequence_number = 0;
	} else if (metadata.iceberg_version == 2) {
		D_ASSERT(snapshot.has_sequence_number);
		ret.sequence_number = snapshot.sequence_number;
	}

	ret.snapshot_id = snapshot.snapshot_id;
	ret.timestamp_ms = Timestamp::FromEpochMs(snapshot.timestamp_ms);
	D_ASSERT(snapshot.has_schema_id);
	ret.schema_id = snapshot.schema_id;
	ret.manifest_list = snapshot.manifest_list;

	//! Parse summary statistics from additional_properties if available
	auto &props = snapshot.summary.additional_properties;
	if (!props.empty()) {
		//! Check if total-records exists (key indicator of statistics presence)
		if (props.find("total-records") != props.end()) {
			ret.summary.has_statistics = true;
			ret.summary.total_records = ParseInt64FromProps(props, "total-records");
			ret.summary.total_data_files = ParseInt64FromProps(props, "total-data-files");
			ret.summary.total_files_size = ParseInt64FromProps(props, "total-files-size");
			ret.summary.total_delete_files = ParseInt64FromProps(props, "total-delete-files");
			ret.summary.total_position_deletes = ParseInt64FromProps(props, "total-position-deletes");
			ret.summary.total_equality_deletes = ParseInt64FromProps(props, "total-equality-deletes");
			ret.summary.added_records = ParseInt64FromProps(props, "added-records");
			ret.summary.added_data_files = ParseInt64FromProps(props, "added-data-files");
			ret.summary.added_files_size = ParseInt64FromProps(props, "added-files-size");
			ret.summary.deleted_records = ParseInt64FromProps(props, "deleted-records");
			ret.summary.deleted_data_files = ParseInt64FromProps(props, "deleted-data-files");
			ret.summary.removed_files_size = ParseInt64FromProps(props, "removed-files-size");
			ret.summary.changed_partition_count = ParseInt64FromProps(props, "changed-partition-count");
		}
	}

	return ret;
}

} // namespace duckdb
