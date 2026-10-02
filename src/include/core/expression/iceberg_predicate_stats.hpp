#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

class BaseStatistics;
class ClientContext;

struct IcebergPredicateStats {
public:
	IcebergPredicateStats() {
	}
	IcebergPredicateStats(const IcebergPredicateStats &other) = default;

public:
	static IcebergPredicateStats DeserializeBounds(ClientContext &context, const Value &lower_bound,
	                                               const Value &upper_bound, const string &name,
	                                               const LogicalType &type);
	void SetLowerBound(const Value &new_lower_bound);
	void SetUpperBound(const Value &new_upper_bound);
	bool BoundsAreNull() const;

public:
	bool has_lower_bounds = false;
	bool has_upper_bounds = false;
	Value lower_bound;
	Value upper_bound;
	bool has_null = false;
	bool has_not_null = false;
	bool has_nan = false;
	//! For GEOMETRY columns: a GEOMETRY_STATS BaseStatistics carrying the file's
	//! bounding-box extent, used to delegate spatial predicate pruning to
	//! GeometryStats::CheckZonemap. Null for non-geometry columns.
	shared_ptr<BaseStatistics> geometry_stats;
};

} // namespace duckdb
