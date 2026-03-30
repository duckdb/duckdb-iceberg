#include "function/ducklake/ducklake_column_stats.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

DuckLakeColumnStats::DuckLakeColumnStats(DuckLakeColumn &column) : column(column) {
}
void DuckLakeColumnStats::AddStats(IcebergPredicateStats &stats) {
	//! Update the stats
	if (stats.has_null) {
		contains_null = true;
	}
	if (stats.has_nan) {
		contains_nan = true;
	}

	if (!stats.lower_bound.IsNull() && (min_value.IsNull() || stats.lower_bound < min_value)) {
		min_value = stats.lower_bound;
	}
	if (!stats.upper_bound.IsNull() && (max_value.IsNull() || stats.upper_bound > max_value)) {
		max_value = stats.upper_bound;
	}
}

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
