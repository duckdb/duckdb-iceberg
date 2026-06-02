#pragma once

#include "core/expression/iceberg_predicate_stats.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/value.hpp"
#include "function/ducklake/ducklake_column.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

struct DuckLakeColumnStats {
public:
	DuckLakeColumnStats(DuckLakeColumn &column);

public:
	void AddStats(IcebergPredicateStats &stats);

public:
	DuckLakeColumn &column;

	bool contains_nan = false;
	bool contains_null = false;
	Value min_value;
	Value max_value;
};

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
