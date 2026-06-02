#pragma once

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

namespace duckdb {

enum class SnapshotSource : uint8_t { LATEST, FROM_TIMESTAMP, FROM_ID };

struct IcebergSnapshotLookup {
public:
	int64_t snapshot_id;
	timestamp_t snapshot_timestamp;

public:
	SnapshotSource GetSource() const {
		return snapshot_source;
	}
	void SetSource(SnapshotSource source) {
		snapshot_source = source;
	}
	bool IsLatest() const {
		return snapshot_source == SnapshotSource::LATEST;
	}
	bool IsFromTimestamp() const {
		return snapshot_source == SnapshotSource::FROM_TIMESTAMP;
	}
	static IcebergSnapshotLookup FromAtClause(optional_ptr<BoundAtClause> at);

private:
	SnapshotSource snapshot_source = SnapshotSource::LATEST;
};

} // namespace duckdb
