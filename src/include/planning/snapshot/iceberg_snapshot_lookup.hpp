#pragma once

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

#include <variant>

namespace duckdb {

enum class SnapshotSource : uint8_t { LATEST, FROM_TIMESTAMP, FROM_ID };

struct IcebergSnapshotLookup {
public:
	using SnapshotFromId = int64_t;
	using SnapshotFromTimestamp = timestamp_ms_t;

	// std::monostate represents "latest" (no extra data needed)
	using variant_t = std::variant<std::monostate, SnapshotFromId, SnapshotFromTimestamp>;

public:
	static IcebergSnapshotLookup FromLatest() {
		return IcebergSnapshotLookup(std::monostate {});
	}
	static IcebergSnapshotLookup FromSnapshotId(int64_t snapshot_id) {
		return IcebergSnapshotLookup(SnapshotFromId(snapshot_id));
	}
	static IcebergSnapshotLookup FromTimestamp(timestamp_ms_t snapshot_timestamp) {
		return IcebergSnapshotLookup(SnapshotFromTimestamp(snapshot_timestamp));
	}
	static IcebergSnapshotLookup FromAtClause(optional_ptr<BoundAtClause> at);

public:
	SnapshotSource GetSource() const {
		if (std::holds_alternative<std::monostate>(snapshot_source)) {
			return SnapshotSource::LATEST;
		}
		if (std::holds_alternative<SnapshotFromId>(snapshot_source)) {
			return SnapshotSource::FROM_ID;
		}
		return SnapshotSource::FROM_TIMESTAMP;
	}
	bool IsLatest() const {
		return std::holds_alternative<std::monostate>(snapshot_source);
	}
	bool IsFromId() const {
		return std::holds_alternative<SnapshotFromId>(snapshot_source);
	}
	bool IsFromTimestamp() const {
		return std::holds_alternative<SnapshotFromTimestamp>(snapshot_source);
	}

	int64_t GetSnapshotId() const {
		return std::get<SnapshotFromId>(snapshot_source);
	}
	timestamp_ms_t GetSnapshotTimestamp() const {
		return std::get<SnapshotFromTimestamp>(snapshot_source);
	}

private:
	explicit IcebergSnapshotLookup(variant_t source) : snapshot_source(std::move(source)) {
	}

private:
	const variant_t snapshot_source;
};

} // namespace duckdb
