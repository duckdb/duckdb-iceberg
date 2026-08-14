//===----------------------------------------------------------------------===//
//                         DuckDB - iceberg
//
// planning/deletes/iceberg_equality_delete_fast_filter.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "core/deletes/iceberg_equality_delete.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"

namespace duckdb {

//! A hash-table fast path for equality deletes. Unsupported layouts remain on
//! the upstream bound-expression path, so this never broadens the supported
//! type surface.
class IcebergEqualityDeleteFastFilter {
public:
	struct Layout {
		vector<idx_t> column_indices;
		vector<LogicalType> types;
		unique_ptr<GroupedAggregateHashTable> hash_table;
	};

	struct BuildResult {
		shared_ptr<IcebergEqualityDeleteFastFilter> filter;
		vector<bool> accelerated_files;
	};

	static BuildResult Build(const vector<reference<const IcebergEqualityDeleteFile>> &delete_files,
	                         const unordered_map<int32_t, idx_t> &field_indexes,
	                         const vector<LogicalType> &target_types, ClientContext &context, Allocator &allocator);

	//! Marks matching rows in `deleted`; it never clears an existing mark.
	void MarkDeleted(DataChunk &keys, vector<bool> &deleted) const;

private:
	vector<Layout> layouts;
};

} // namespace duckdb
