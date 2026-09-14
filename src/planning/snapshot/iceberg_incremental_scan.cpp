#include "planning/snapshot/iceberg_incremental_scan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/vector.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"

namespace duckdb {

IcebergIncrementalSnapshots IcebergIncrementalSnapshots::Resolve(const IcebergTableMetadata &metadata,
                                                                 const IcebergIncrementalScanRange &range) {
	D_ASSERT(range.IsIncremental());
	auto start_snapshot_id = *range.start_snapshot_id;

	int64_t end_snapshot_id;
	if (range.end_snapshot_id) {
		end_snapshot_id = *range.end_snapshot_id;
	} else if (metadata.current_snapshot_id) {
		end_snapshot_id = *metadata.current_snapshot_id;
	} else {
		throw InvalidInputException("'end_snapshot_id' is not set and the table has no current snapshot");
	}

	//! Both throw when the snapshot does not exist
	metadata.GetSnapshotById(start_snapshot_id);
	auto end_snapshot = metadata.GetSnapshotById(end_snapshot_id);

	IcebergIncrementalSnapshots result;
	result.end_snapshot_id = end_snapshot_id;
	if (start_snapshot_id == end_snapshot_id) {
		//! An empty range, the scan produces no files
		return result;
	}

	//! 'start_snapshot_id' is exclusive, so it is never collected.
	vector<reference<const IcebergSnapshot>> collected;
	optional_ptr<const IcebergSnapshot> cursor = end_snapshot;
	bool found_start = false;
	//! The bound stops a cycle in corrupt metadata
	for (idx_t step = 0; cursor && step <= metadata.snapshots.size(); step++) {
		collected.push_back(*cursor);
		if (!cursor->parent_snapshot_id) {
			//! Reached the root of the chain without passing the start snapshot
			break;
		}
		auto parent_snapshot_id = *cursor->parent_snapshot_id;
		if (parent_snapshot_id == start_snapshot_id) {
			found_start = true;
			break;
		}
		cursor = metadata.FindSnapshotByIdInternal(parent_snapshot_id);
	}
	if (!found_start) {
		throw InvalidInputException(
		    "'start_snapshot_id' %lld is not an ancestor of 'end_snapshot_id' %lld, so the range of "
		    "appended data between them is not defined",
		    start_snapshot_id, end_snapshot_id);
	}

	for (auto &snapshot_ref : collected) {
		auto &snapshot = snapshot_ref.get();
		if (!snapshot.snapshot_id) {
			throw InvalidConfigurationException("Snapshot in the requested range has no 'snapshot-id'");
		}
		if (snapshot.operation != IcebergSnapshotOperationType::APPEND) {
			throw InvalidInputException(
			    "Snapshot %lld in the requested range has operation '%s', incremental reads only support "
			    "'append' snapshots. Reading between these snapshots would silently skip the data it changed",
			    *snapshot.snapshot_id, IcebergSnapshotOperationTypeToString(snapshot.operation));
		}
		result.snapshot_ids.insert(*snapshot.snapshot_id);
	}
	return result;
}

bool IcebergIncrementalSnapshots::ManifestInRange(const IcebergManifestFile &manifest_file) const {
	if (!manifest_file.added_snapshot_id) {
		//! Not attributable, leave the decision to the entry check
		return true;
	}
	return snapshot_ids.count(*manifest_file.added_snapshot_id) != 0;
}

bool IcebergIncrementalSnapshots::EntryInRange(const IcebergManifestEntry &entry,
                                               const IcebergManifestFile &manifest_file) const {
	if (entry.status != IcebergManifestEntryStatusType::ADDED) {
		//! An EXISTING entry was carried forward from an earlier snapshot, it is not new data
		return false;
	}
	return snapshot_ids.count(entry.GetSnapshotId(manifest_file)) != 0;
}

} // namespace duckdb
