#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

namespace duckdb {

static string VERSION_GUESSING_CONFIG_VARIABLE = "unsafe_enable_version_guessing";

// When this is provided (and unsafe_enable_version_guessing is true)
// we first look for DEFAULT_VERSION_HINT_FILE, if it doesn't exist we
// then search for versions matching the DEFAULT_TABLE_VERSION_FORMAT
// We take the lexographically "greatest" one as the latest version
// Note that this will voliate ACID constraints in some situations.
static string UNKNOWN_TABLE_VERSION = "?";

// First arg is version string, arg is either empty or ".gz" if gzip
// Allows for both "v###.gz.metadata.json" and "###.metadata.json" styles
static string DEFAULT_TABLE_VERSION_FORMAT = "v%s%s.metadata.json,%s%s.metadata.json";

// This isn't explicitly in the standard, but is a commonly used technique
static string DEFAULT_VERSION_HINT_FILE = "version-hint.text";

// By default we will use the unknown version behavior mentioned above
static string DEFAULT_TABLE_VERSION = UNKNOWN_TABLE_VERSION;

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

struct IcebergOptions {
	bool allow_moved_paths = false;
	string metadata_compression_codec = "none";
	bool infer_schema = true;
	string table_version = DEFAULT_TABLE_VERSION;
	string version_name_format = DEFAULT_TABLE_VERSION_FORMAT;

	IcebergSnapshotLookup snapshot_lookup;
};

} // namespace duckdb
