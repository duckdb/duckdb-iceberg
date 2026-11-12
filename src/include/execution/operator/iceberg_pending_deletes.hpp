#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Newly selected positions, before adding existing deletes or writing any files.
//! Keep duplicates until materialization so duplicate UPDATE matches are still rejected.
struct IcebergPendingDeletes {
	unordered_map<string, vector<idx_t>> rows_by_file;

	bool IsEmpty() const {
		return rows_by_file.empty();
	}
};

} // namespace duckdb
