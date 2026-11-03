#pragma once

#include "duckdb/planner/filter/expression_filter.hpp"

#include "core/expression/iceberg_transform.hpp"
#include "core/expression/iceberg_predicate_stats.hpp"

namespace duckdb {

//! Combined result mirroring Iceberg's InclusiveMetricsEvaluator and StrictMetricsEvaluator.
enum class IcebergMetricsMatch : uint8_t {
	//! The inclusive evaluator rules the file out.
	ROWS_CANNOT_MATCH,
	//! Neither ruled out nor proven: read the rows and apply the filter to each.
	ROWS_MIGHT_MATCH,
	//! The strict evaluator proves that every row matches.
	ROWS_MUST_MATCH
};

struct IcebergPredicate {
public:
	IcebergPredicate() = delete;

public:
	//! Asks the inclusive question, and also the strict one when `evaluate_strict` (scans only need the former).
	static IcebergMetricsMatch MatchBounds(ClientContext &context, const ExpressionFilter &filter,
	                                       const IcebergPredicateStats &stats, const IcebergTransform &transform,
	                                       bool evaluate_strict = false);
};

} // namespace duckdb
