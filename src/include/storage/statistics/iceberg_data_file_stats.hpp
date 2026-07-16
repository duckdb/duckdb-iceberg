//===----------------------------------------------------------------------===//
//! Shared helpers for populating Iceberg data-file column metrics from COPY
//! RETURN_STATS. Used by INSERT and iceberg_rewrite_data_files.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

struct IcebergDataFileStats {
	//! Populate lower/upper bounds, value/null counts, and column sizes on
	//! `data_file` from one COPY RETURN_STATS `column_statistics` map value.
	//! Respects write.metadata.metrics.* and enforces NOT NULL constraints.
	static void PopulateFromReturnStats(ClientContext &context, IcebergDataFile &data_file, const Value &column_stats,
	                                    const IcebergTableMetadata &table_metadata, const string &table_name);
};

} // namespace duckdb
