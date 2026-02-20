#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "deletes/iceberg_delete_data.hpp"

namespace duckdb {

//! Metadata about an altered delete file
struct IcebergDataFileDeletes {
public:
	IcebergDataFileDeletes(IcebergDeleteType type) : type(type) {
	}

public:
	const IcebergDeleteType type;
	//! The parts of the delete files that are invalidated
	//! (should be dropped entirely if this contains *all* the referenced data files)
	vector<string> referenced_data_files;
};

struct IcebergManifestDeletes {
public:
	IcebergManifestDeletes() {
	}

public:
	//! The 'data_file.file_path' of invalidated data files
	case_insensitive_map_t<IcebergDataFileDeletes> altered_data_files;
};

} // namespace duckdb
