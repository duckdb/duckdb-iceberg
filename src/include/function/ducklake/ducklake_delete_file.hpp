#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "function/ducklake/ducklake_data_file.hpp"
#include "function/ducklake/ducklake_snapshot.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

struct DuckLakeDeleteFile {
public:
	DuckLakeDeleteFile(const IcebergManifestEntry &manifest_entry, const string &table_name);

public:
	string FinalizeEntry(int64_t table_id, vector<DuckLakeDataFile> &all_data_files,
	                     const map<timestamp_t, DuckLakeSnapshot> &snapshots);

public:
	string path;
	int64_t record_count;
	int64_t file_size_bytes;
	string data_file_path;

	//! The id is assigned after we've processed all tables
	optional_idx delete_file_id;
	idx_t file_id_offset = DConstants::INVALID_INDEX;

	timestamp_t start_snapshot;
	bool has_end = false;
	timestamp_t end_snapshot;

	//! Index into 'all_data_files'
	idx_t referenced_data_file;
};

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
