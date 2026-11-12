#pragma once

#include "core/deletes/iceberg_delete_file.hpp"
#include "planning/iceberg_row_lineage.hpp"

namespace duckdb {

//! Resolved file metadata, sufficient to enumerate and open a data file.
struct IcebergDataFileDescriptor {
	//! Path used to open the file, possibly relocated by allow_moved_paths.
	string file_path;
	//! Original Iceberg path used to match positional deletes.
	string original_file_path;
	string file_format;
	int64_t file_size_in_bytes = 0;
	int64_t record_count = 0;
	optional<int64_t> sequence_number;
	optional<int64_t> first_row_id;
	IcebergRowLineageMode row_lineage_mode = IcebergRowLineageMode::COMMITTED;
	int32_t partition_spec_id = 0;
};

//! Complete materialized scan input, with no references to planner-owned manifests.
struct IcebergFileScanTask : public IcebergDataFileDescriptor {
	IcebergFileScanTask() = default;
	explicit IcebergFileScanTask(IcebergDataFileDescriptor file) : IcebergDataFileDescriptor(std::move(file)) {
	}

	unordered_map<int32_t, Value> partition_constants;
	vector<IcebergDeleteFile> delete_files;
};

} // namespace duckdb
