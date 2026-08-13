//===----------------------------------------------------------------------===//
//                         DuckDB - iceberg
//
// planning/deletes/iceberg_equality_delete_fast_filter.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "core/deletes/iceberg_equality_delete.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/arena_allocator.hpp"

namespace duckdb {

//! A fast path for equality deletes whose fields have byte-comparable
//! physical representations. Unsupported layouts remain on the upstream bound-
//! expression path, so this never broadens the supported type surface.
class IcebergEqualityDeleteFastFilter {
public:
	static constexpr idx_t SHARD_COUNT = 64;

	struct FlatShard {
		struct Entry {
			hash_t hash = 0;
			string_t key;
			bool occupied = false;
		};

		void Initialize(idx_t expected_count);
		void Insert(hash_t hash, string_t key);
		bool Contains(hash_t hash, string_t key) const;

		vector<Entry> entries;
		idx_t capacity_mask = 0;
	};

	struct Layout {
		vector<idx_t> column_indices;
		vector<LogicalType> types;
		vector<FlatShard> shards = vector<FlatShard>(SHARD_COUNT);
		bool has_null_key = false;
		idx_t key_count = 0;
	};

	struct BuildResult {
		shared_ptr<IcebergEqualityDeleteFastFilter> filter;
		vector<bool> accelerated_files;
	};

	explicit IcebergEqualityDeleteFastFilter(Allocator &allocator) : arena(allocator) {
	}

	static BuildResult Build(const vector<reference<const IcebergEqualityDeleteFile>> &delete_files,
	                         const unordered_map<int32_t, idx_t> &field_indexes,
	                         const vector<LogicalType> &target_types);

	//! Marks matching rows in `deleted`; it never clears an existing mark.
	void MarkDeleted(DataChunk &keys, vector<bool> &deleted) const;

private:
	ArenaAllocator arena;
	vector<Layout> layouts;
};

} // namespace duckdb
