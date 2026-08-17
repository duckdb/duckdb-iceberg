//===----------------------------------------------------------------------===//
//                         DuckDB - iceberg
//
// planning/deletes/iceberg_equality_delete_fast_filter.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "core/deletes/iceberg_equality_delete.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"

#include <condition_variable>

namespace duckdb {

struct ProbeColumn {
	int32_t field_id;
	LogicalType type;
	bool locally_present;

	bool operator==(const ProbeColumn &other) const {
		return field_id == other.field_id && type == other.type && locally_present == other.locally_present;
	}
};

struct EqualityDeleteFastFilterKey {
	vector<const IcebergEqualityDeleteFile *> delete_files;
	vector<ProbeColumn> columns;

	bool operator==(const EqualityDeleteFastFilterKey &other) const {
		return delete_files == other.delete_files && columns == other.columns;
	}
};

} // namespace duckdb

namespace std {

template <>
struct hash<duckdb::EqualityDeleteFastFilterKey> {
	size_t operator()(const duckdb::EqualityDeleteFastFilterKey &key) const {
		duckdb::hash_t result = hash<size_t>()(key.delete_files.size());
		for (auto delete_file : key.delete_files) {
			result = duckdb::CombineHash(result, duckdb::Hash<uint64_t>(reinterpret_cast<uintptr_t>(delete_file)));
		}
		result = duckdb::CombineHash(result, hash<size_t>()(key.columns.size()));
		for (auto &column : key.columns) {
			result = duckdb::CombineHash(result, duckdb::Hash<int32_t>(column.field_id));
			result = duckdb::CombineHash(result, column.type.Hash());
			result = duckdb::CombineHash(result, duckdb::Hash<bool>(column.locally_present));
		}
		return result;
	}
};

} // namespace std

namespace duckdb {

class IcebergEqualityDeleteFastFilterCache;

//! A hash-table fast path for equality deletes. Unsupported layouts remain on
//! the upstream bound-expression path, so this never broadens the supported
//! type surface.
class IcebergEqualityDeleteFastFilter {
public:
	struct BuildResult {
		shared_ptr<IcebergEqualityDeleteFastFilter> filter;
		vector<bool> accelerated_files;
	};

	//! Removes matching rows from `sel` and returns the number of remaining rows.
	idx_t Filter(DataChunk &keys, SelectionVector &sel, idx_t count) const;

private:
	friend class IcebergEqualityDeleteFastFilterCache;

	struct Layout {
		vector<LogicalType> types;
		unique_ptr<GroupedAggregateHashTable> hash_table;
	};

	struct ProbeLayout {
		vector<idx_t> column_indices;
		shared_ptr<const Layout> layout;
	};

	struct LayoutBuildResult {
		shared_ptr<const Layout> layout;
		vector<bool> accelerated_files;
	};

	vector<ProbeLayout> layouts;
};

class IcebergEqualityDeleteFastFilterCache {
public:
	IcebergEqualityDeleteFastFilter::BuildResult
	GetOrCreate(const vector<reference<const IcebergEqualityDeleteFile>> &delete_files,
	            const unordered_map<int32_t, idx_t> &field_indexes, const vector<LogicalType> &target_types,
	            const set<int32_t> &local_field_ids, ClientContext &context, Allocator &allocator);

private:
	//! One builder publishes each layout key; waiters share its result while unrelated layouts build concurrently.
	struct LayoutLoadState {
		mutex lock;
		std::condition_variable cv;
		bool complete = false;
		ErrorData error;
		IcebergEqualityDeleteFastFilter::LayoutBuildResult result;
	};

	annotated_mutex lock;
	unordered_map<EqualityDeleteFastFilterKey, shared_ptr<LayoutLoadState>> layouts DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
