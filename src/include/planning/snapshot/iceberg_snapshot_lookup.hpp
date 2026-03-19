#pragma once

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

namespace duckdb {

enum class SnapshotSource : uint8_t { LATEST, FROM_TIMESTAMP, FROM_ID };

struct IcebergSnapshotLookup {
public:
	SnapshotSource snapshot_source = SnapshotSource::LATEST;
	int64_t snapshot_id;
	timestamp_t snapshot_timestamp;

public:
	bool IsLatest() const {
		return snapshot_source == SnapshotSource::LATEST;
	}
	static IcebergSnapshotLookup FromAtClause(optional_ptr<BoundAtClause> at);
};

} // namespace duckdb
