//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/statistics/iceberg_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

struct IcebergColumnStats {
	explicit IcebergColumnStats(LogicalType type_p) : type(std::move(type_p)) {
	}

	// Copy constructor
	IcebergColumnStats(const IcebergColumnStats &other);
	IcebergColumnStats &operator=(const IcebergColumnStats &other);
	IcebergColumnStats(IcebergColumnStats &&other) noexcept = default;
	IcebergColumnStats &operator=(IcebergColumnStats &&other) noexcept = default;

	LogicalType type;
	optional<string> min;
	optional<string> max;
	optional<idx_t> null_count;
	optional<idx_t> num_values;
	optional<idx_t> column_size_bytes;
	optional<bool> contains_nan;
	bool any_valid = true;

	// Geometry bounding-box stats produced by the parquet writer's RETURN_STATS.
	// Z/M halves are only set when the parquet writer emitted them, which it does
	// iff the data actually contained those dimensions.
	bool has_bbox_xy = false;
	double bbox_xmin = 0.0;
	double bbox_xmax = 0.0;
	double bbox_ymin = 0.0;
	double bbox_ymax = 0.0;
	bool has_bbox_z = false;
	double bbox_zmin = 0.0;
	double bbox_zmax = 0.0;
	bool has_bbox_m = false;
	double bbox_mmin = 0.0;
	double bbox_mmax = 0.0;

public:
	unique_ptr<BaseStatistics> ToStats() const;
	void MergeStats(const IcebergColumnStats &new_stats);
	IcebergColumnStats Copy() const;
	static IcebergColumnStats ParseColumnStats(const LogicalType &type, const vector<Value> &col_stats,
	                                           ClientContext &context);

private:
	unique_ptr<BaseStatistics> CreateNumericStats() const;
	unique_ptr<BaseStatistics> CreateStringStats() const;
};

} // namespace duckdb
