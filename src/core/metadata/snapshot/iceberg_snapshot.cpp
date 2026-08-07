#include "core/metadata/snapshot/iceberg_snapshot.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
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

rest_api_objects::Snapshot IcebergSnapshot::ToRESTObject(const IcebergTableMetadata &table_metadata) const {
	rest_api_objects::Snapshot res;

	if (!snapshot_id) {
		throw InvalidConfigurationException("snapshot.snapshot_id is not set");
	}
	res.snapshot_id = *snapshot_id;
	res.timestamp_ms = timestamp_ms.value;
	if (manifest_list.empty()) {
		throw InvalidConfigurationException("Writing snapshots without a manifest list is not supported");
	}
	res.manifest_list = manifest_list;

	res.summary.operation = OperationTypeToString(operation);
	res.summary.additional_properties = metrics.ToString();

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
	if (snapshot.manifest_list) {
		ret.manifest_list = *snapshot.manifest_list;
	} else if (snapshot.manifests) {
		ret.manifests = *snapshot.manifests;
	} else {
		throw InvalidConfigurationException("Snapshot must contain either 'manifest-list' or 'manifests'");
	}
	ret.metrics = IcebergSnapshotMetrics(snapshot.summary.additional_properties);

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
