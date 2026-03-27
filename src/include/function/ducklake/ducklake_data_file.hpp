#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "function/ducklake/ducklake_partition.hpp"
#include "function/ducklake/ducklake_snapshot.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

struct DuckLakeDataFile {
public:
	DuckLakeDataFile(const IcebergManifestEntry &manifest_entry_p, DuckLakePartition &partition,
	                 const string &table_name);

public:
	string FinalizeEntry(int64_t table_id, const map<timestamp_t, DuckLakeSnapshot> &snapshots);

public:
	//! Contains the stats used to write the 'ducklake_file_column_stats'
	IcebergManifestEntry manifest_entry;
	DuckLakePartition &partition;

	string path;
	int64_t record_count;
	int64_t file_size_bytes;

	//! The id is assigned after we've processed all tables
	optional_idx data_file_id;
	idx_t file_id_offset = DConstants::INVALID_INDEX;

	timestamp_t start_snapshot;

	bool has_end = false;
	timestamp_t end_snapshot;
};

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
