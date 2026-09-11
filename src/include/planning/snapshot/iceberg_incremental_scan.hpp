#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

struct IcebergManifestEntry;
struct IcebergManifestFile;
struct IcebergTableMetadata;

//! The unresolved 'start_snapshot_id' / 'end_snapshot_id' options of 'iceberg_scan'.
struct IcebergIncrementalScanRange {
public:
	//! Exclusive lower bound. Its presence is what makes a scan incremental.
	optional<int64_t> start_snapshot_id;
	//! Inclusive upper bound. When unset the table's current snapshot is used.
	optional<int64_t> end_snapshot_id;

public:
	bool IsIncremental() const {
		return start_snapshot_id.has_value();
	}
	//! Whether either bound was supplied, used to reject combining a range with a point-in-time read.
	bool IsSet() const {
		return start_snapshot_id.has_value() || end_snapshot_id.has_value();
	}
};

//! The resolved ancestor chain between the two bounds. Resolved during bind from already parsed
//! metadata, so it reads no files.
struct IcebergIncrementalSnapshots {
public:
	//! Throws when a bound does not exist, when start is not an ancestor of end, or when a snapshot
	//! in the range is not an append.
	static IcebergIncrementalSnapshots Resolve(const IcebergTableMetadata &metadata,
	                                           const IcebergIncrementalScanRange &range);

public:
	//! An unattributable manifest (no 'added_snapshot_id') returns true, the entry check decides.
	bool ManifestInRange(const IcebergManifestFile &manifest_file) const;
	//! Whether a manifest entry was added by a snapshot in the range.
	bool EntryInRange(const IcebergManifestEntry &entry, const IcebergManifestFile &manifest_file) const;

public:
	//! Ancestors of the end snapshot down to, but excluding, the start snapshot.
	unordered_set<int64_t> snapshot_ids;
	//! The resolved inclusive upper bound, whose manifest list and schema the scan reads.
	int64_t end_snapshot_id = 0;
};

} // namespace duckdb
