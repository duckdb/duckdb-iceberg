#pragma once

#include "duckdb/common/mutex.hpp"
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

//! The rows the UPDATE and DELETE actions of a copy-on-write MERGE remove. The actions add them instead of writing
//! delete files, and the rewrite that runs after the MERGE drops them from the files it rewrites.
struct IcebergCopyOnWriteMergeRows {
	mutex lock;
	IcebergPendingDeletes deletes;

	void Add(IcebergPendingDeletes &&pending) {
		lock_guard<mutex> guard(lock);
		for (auto &entry : pending.rows_by_file) {
			auto &rows = deletes.rows_by_file[entry.first];
			rows.insert(rows.end(), entry.second.begin(), entry.second.end());
		}
	}
};

} // namespace duckdb
