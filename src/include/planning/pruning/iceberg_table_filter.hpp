#pragma once

#include "duckdb/common/column_index_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

struct IcebergTableFilters {
	using filter_set_t = column_index_map<unique_ptr<ExpressionFilter>>;
	using iterator = filter_set_t::iterator;
	using const_iterator = filter_set_t::const_iterator;

public:
	bool HasFilters() const {
		return !table_filters.empty();
	}
	idx_t FilterCount() const {
		return table_filters.size();
	}
	void PushFilter(const ColumnIndex &column_idx, unique_ptr<ExpressionFilter> table_filter) {
		D_ASSERT(table_filters.find(column_idx) == table_filters.end());
		table_filters[column_idx] = std::move(table_filter);
		partition_filters.clear();
	}
	optional_ptr<const ExpressionFilter> TryGetFilterByColumnIndex(const ColumnIndex &column_idx) const {
		auto entry = table_filters.find(column_idx);
		if (entry == table_filters.end()) {
			return nullptr;
		}
		return entry->second.get();
	}
	//! Resolve a partition source path from IcebergTableSchema::GetSourceIdMap.
	//! Borrows the top-level filter or an owned, lazily extracted nested-path filter.
	//! The result remains valid until this filter set is changed or destroyed.
	//! Nested-path publication has its own lock; the filter set is frozen before planning.
	optional_ptr<const ExpressionFilter> GetFilterForColumnIndex(const ColumnIndex &column_index) const;
	static bool FiltersEqual(const ExpressionFilter &left, const ExpressionFilter &right);

	iterator begin() { // NOLINT: match stl API
		return table_filters.begin();
	}
	iterator end() { // NOLINT: match stl API
		return table_filters.end();
	}
	const_iterator begin() const { // NOLINT: match stl API
		return table_filters.begin();
	}
	const_iterator end() const { // NOLINT: match stl API
		return table_filters.end();
	}

private:
	filter_set_t table_filters;
	//! Per-view nested partition paths, including nullptr for paths that cannot be extracted.
	mutable filter_set_t partition_filters;
	unique_ptr<mutex> partition_filter_lock = make_uniq<mutex>();
};

} // namespace duckdb
