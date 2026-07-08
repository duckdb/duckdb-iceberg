#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/optional.hpp"

namespace duckdb {

class BaseStatistics;

struct IcebergPredicateStats {
public:
	IcebergPredicateStats() {
	}
	IcebergPredicateStats(const IcebergPredicateStats &other) = default;

public:
	static IcebergPredicateStats DeserializeBounds(const Value &lower_bound, const Value &upper_bound,
	                                               const string &name, const LogicalType &type);
	void SetLowerBound(const Value &new_lower_bound);
	void SetUpperBound(const Value &new_upper_bound);
	bool BoundsAreNull() const;

public:
	optional<Value> lower_bound;
	optional<Value> upper_bound;
	bool has_null = true;
	bool has_not_null = true;
	bool has_nan = true;
	//! For GEOMETRY columns: a GEOMETRY_STATS BaseStatistics carrying the file's
	//! bounding-box extent, used to delegate spatial predicate pruning to
	//! GeometryStats::CheckZonemap. Null for non-geometry columns.
	shared_ptr<BaseStatistics> geometry_stats;
};

} // namespace duckdb
