#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "planning/snapshot/iceberg_snapshot_lookup.hpp"

namespace duckdb {

static string VERSION_GUESSING_CONFIG_VARIABLE = "unsafe_enable_version_guessing";

// The Iceberg format version used when creating a new table without an explicit
// 'format-version' property. Valid values are 2 and 3. Version 1 is not supported
// for writing (NotImplementedException); anything outside [2, 3] is rejected
// (InvalidConfigurationException).
static string DEFAULT_FORMAT_VERSION_CONFIG_VARIABLE = "iceberg_default_format_version";
static constexpr uint64_t DEFAULT_ICEBERG_FORMAT_VERSION = 2;

// When this is true, a DELETE on a v2 Iceberg table whose WHERE clause is a pure
// conjunction of equality predicates writes an Iceberg equality-delete file instead
// of a positional delete. This exists only to exercise the equality-delete read path.
static string ENABLE_EQUALITY_DELETES_CONFIG_VARIABLE = "unsafe_and_disabled_for_iceberg_v3_enable_equality_deletes";

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

struct IcebergOptions {
public:
	IcebergOptions();
	IcebergOptions(named_parameter_map_t &named_parameters);
	IcebergOptions(const IcebergOptions &) = default;
	IcebergOptions &operator=(const IcebergOptions &other);

public:
	bool allow_moved_paths = false;
	string metadata_compression_codec = "none";
	bool infer_schema = true;
	string table_version = DEFAULT_TABLE_VERSION;
	string version_name_format = DEFAULT_TABLE_VERSION_FORMAT;

	optional<IcebergSnapshotLookup> snapshot_lookup;
};

} // namespace duckdb
