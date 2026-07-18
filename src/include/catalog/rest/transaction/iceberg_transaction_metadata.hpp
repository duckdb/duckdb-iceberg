#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "core/deletes/iceberg_delete_data.hpp"

namespace duckdb {

struct IcebergManifestDeletes {
public:
	void InvalidateFile(const string &file_path) {
		data_files.insert(file_path);
	}
	//! Records how many still-live rows a metadata-only DELETE removed from this data file
	void InvalidateFile(const string &file_path, idx_t live_rows_removed) {
		data_files.insert(file_path);
		data_file_live_rows[file_path] = live_rows_removed;
	}
	bool IsInvalidated(const string &file_path) const {
		return data_files.count(file_path);
	}
	bool IsEmpty() const {
		return data_files.empty();
	}
	const unordered_set<string> &InvalidatedFiles() const {
		return data_files;
	}
	const case_insensitive_map_t<idx_t> &InvalidatedDataFileLiveRows() const {
		return data_file_live_rows;
	}

private:
	//! The 'data_file.file_path' of invalidated data files
	unordered_set<string> data_files;
	//! Live rows removed per invalidated data file, for the subset with a known count (see InvalidateFile)
	case_insensitive_map_t<idx_t> data_file_live_rows;
};

} // namespace duckdb
