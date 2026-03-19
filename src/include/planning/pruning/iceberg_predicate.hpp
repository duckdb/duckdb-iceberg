#pragma once

#include "duckdb/planner/table_filter.hpp"

#include "core/expression/iceberg_transform.hpp"
#include "core/expression/iceberg_predicate_stats.hpp"

namespace duckdb {

struct IcebergPredicate {
public:
	IcebergPredicate() = delete;

public:
	static bool MatchBounds(ClientContext &context, const TableFilter &filter, const IcebergPredicateStats &stats,
	                        const IcebergTransform &transform);
};

} // namespace duckdb
