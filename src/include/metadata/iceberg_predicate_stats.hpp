#pragma once
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct IcebergPredicateStats {
public:
	IcebergPredicateStats() {
	}
	IcebergPredicateStats(const IcebergPredicateStats &other) = default;

public:
	static IcebergPredicateStats DeserializeBounds(const Value &lower_bound, const Value &upper_bound,
	                                               const string &name, const LogicalType &type);

public:
	Value lower_bound;
	Value upper_bound;
	bool has_null = false;
	bool has_not_null = false;
	bool has_nan = false;
};

} // namespace duckdb
