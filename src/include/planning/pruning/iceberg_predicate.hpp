#pragma once

#include "duckdb/planner/filter/expression_filter.hpp"

#include "core/expression/iceberg_transform.hpp"
#include "core/expression/iceberg_predicate_stats.hpp"

namespace duckdb {

//! What a set of bounds proves about a filter.
enum class METADATA_STATS_PUSHDOWN : uint8_t {
	//! No row can match, so whatever the bounds describe never has to be read.
	NO_ROWS_MATCH,
	//! Neither ruled out nor provably covered: read the rows and apply the filter to each.
	SOME_ROWS_MATCH,
	//! Every row provably matches, so a DELETE can drop what the bounds describe without reading it.
	ALL_ROWS_MATCH
};

struct IcebergPredicate {
public:
	IcebergPredicate() = delete;

public:
	//! Answers both halves in one walk of `filter`: could any row described by `stats` match, and must
	//! every row match.
	static METADATA_STATS_PUSHDOWN MatchBounds(ClientContext &context, const ExpressionFilter &filter,
	                                           const IcebergPredicateStats &stats, const IcebergTransform &transform);
};

} // namespace duckdb
