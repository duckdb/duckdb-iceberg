#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "core/deletes/iceberg_delete_data.hpp"

namespace duckdb {

struct IcebergManifestDeletes {
public:
	void InvalidateFile(const string &file_path) {
		data_files.insert(file_path);
	}
	bool IsInvalidated(const string &file_path) const {
		return data_files.count(file_path);
	}
	bool IsEmpty() const {
		return data_files.empty();
	}

private:
	//! The 'data_file.file_path' of invalidated data files
	unordered_set<string> data_files;
};

} // namespace duckdb
