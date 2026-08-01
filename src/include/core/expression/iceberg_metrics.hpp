#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

struct IcebergTableMetadata;

// Default string bound truncation length — Iceberg's truncate(16) default.
static constexpr idx_t DEFAULT_METRICS_TRUNCATE_LENGTH = 16;

// Per-column metrics collection level, from the write.metadata.metrics.* table
// properties (https://iceberg.apache.org/docs/latest/configuration/#write-properties).
enum class IcebergMetricsMode : uint8_t { NONE, COUNTS, TRUNCATE, FULL };

struct IcebergMetricsConfig {
	IcebergMetricsMode mode = IcebergMetricsMode::TRUNCATE;
	// Byte length for TRUNCATE; DConstants::INVALID_INDEX for FULL (no truncation).
	idx_t truncate_length = DEFAULT_METRICS_TRUNCATE_LENGTH;

	IcebergMetricsConfig() = default;
	// C++14: brace-init of a type with default member initializers fails on CI compilers.
	IcebergMetricsConfig(IcebergMetricsMode mode_p, idx_t truncate_length_p)
	    : mode(mode_p), truncate_length(truncate_length_p) {
	}
};

// Parse a metrics mode value: none, counts, truncate(<n>), or full.
IcebergMetricsConfig ParseMetricsMode(const string &raw);
// Table-level default from write.metadata.metrics.default (truncate(16) when unset).
IcebergMetricsConfig GetDefaultMetricsConfig(const IcebergTableMetadata &table_metadata);
// Per-column config: write.metadata.metrics.column.<name> override, else default_config.
IcebergMetricsConfig GetColumnMetricsConfig(const IcebergTableMetadata &table_metadata,
                                            const IcebergMetricsConfig &default_config, const string &column_name);

} // namespace duckdb
