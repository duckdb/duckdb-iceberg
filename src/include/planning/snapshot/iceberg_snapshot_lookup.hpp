#pragma once

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

#include <variant>

namespace duckdb {

enum class SnapshotSource : uint8_t { LATEST, FROM_TIMESTAMP, FROM_ID, FROM_REF };

struct IcebergSnapshotLookup {
public:
	static constexpr const char *EXCLUSIVE_OPTIONS_ERROR =
	    "Only one of 'snapshot_from_id', 'snapshot_from_timestamp' and 'snapshot_from_ref' can be used";
	//! Throws if a snapshot option value is NULL
	static void VerifyOptionNotNull(const string &option, const Value &value);

public:
	using SnapshotFromId = int64_t;
	using SnapshotFromTimestamp = timestamp_ms_t;
	//! A branch or tag, resolved through the table's 'refs'
	struct SnapshotFromRef {
		string name;
	};

	// std::monostate represents "latest" (no extra data needed)
	using variant_t = std::variant<std::monostate, SnapshotFromId, SnapshotFromTimestamp, SnapshotFromRef>;

public:
	static IcebergSnapshotLookup FromLatest() {
		return IcebergSnapshotLookup(std::monostate {});
	}
	static IcebergSnapshotLookup FromSnapshotId(int64_t snapshot_id) {
		return IcebergSnapshotLookup(SnapshotFromId(snapshot_id));
	}
	//! Throws if 'snapshot_timestamp' lies beyond the current time
	static IcebergSnapshotLookup FromTimestamp(timestamp_ms_t snapshot_timestamp);
	//! Throws if 'ref_name' is empty
	static IcebergSnapshotLookup FromRef(const string &ref_name);
	//! A VERSION value: an integer, or a string of an optional sign and ASCII digits in int64 range, is a
	//! snapshot id; any other string is a ref name. A ref named like a number is only reachable through
	//! 'snapshot_from_ref'.
	static IcebergSnapshotLookup FromVersion(const Value &value);
	static IcebergSnapshotLookup FromAtClause(optional_ptr<BoundAtClause> at);

public:
	SnapshotSource GetSource() const {
		if (std::holds_alternative<std::monostate>(snapshot_source)) {
			return SnapshotSource::LATEST;
		}
		if (std::holds_alternative<SnapshotFromId>(snapshot_source)) {
			return SnapshotSource::FROM_ID;
		}
		if (std::holds_alternative<SnapshotFromRef>(snapshot_source)) {
			return SnapshotSource::FROM_REF;
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
	bool IsFromRef() const {
		return std::holds_alternative<SnapshotFromRef>(snapshot_source);
	}

	int64_t GetSnapshotId() const {
		return std::get<SnapshotFromId>(snapshot_source);
	}
	timestamp_ms_t GetSnapshotTimestamp() const {
		return std::get<SnapshotFromTimestamp>(snapshot_source);
	}
	const SnapshotFromRef &GetSnapshotRef() const {
		return std::get<SnapshotFromRef>(snapshot_source);
	}

private:
	explicit IcebergSnapshotLookup(variant_t source) : snapshot_source(std::move(source)) {
	}

private:
	const variant_t snapshot_source;
};

} // namespace duckdb
