#pragma once
#include "metadata/iceberg_transform.hpp"
#include "metadata/iceberg_predicate_stats.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

struct IcebergPredicate {
public:
	IcebergPredicate() = delete;

public:
	static bool MatchBounds(ClientContext &context, const TableFilter &filter, const IcebergPredicateStats &stats,
	                        const IcebergTransform &transform);
};

} // namespace duckdb
