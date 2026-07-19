#include "storage/statistics/iceberg_statistics.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

IcebergColumnStats IcebergColumnStats::ParseColumnStats(const LogicalType &type, const vector<Value> &col_stats,
                                                        ClientContext &context) {
	IcebergColumnStats column_stats(type);
	for (idx_t stats_idx = 0; stats_idx < col_stats.size(); stats_idx++) {
		auto &stats_children = StructValue::GetChildren(col_stats[stats_idx]);
		auto &stats_name = StringValue::Get(stats_children[0]);
		if (stats_name == "min") {
			D_ASSERT(!column_stats.min);
			column_stats.min = StringValue::Get(stats_children[1]);
		} else if (stats_name == "max") {
			D_ASSERT(!column_stats.max);
			column_stats.max = StringValue::Get(stats_children[1]);
		} else if (stats_name == "null_count") {
			D_ASSERT(!column_stats.null_count);
			column_stats.null_count = StringUtil::ToUnsigned(StringValue::Get(stats_children[1]));
		} else if (stats_name == "num_values") {
			D_ASSERT(!column_stats.num_values);
			column_stats.num_values = StringUtil::ToUnsigned(StringValue::Get(stats_children[1]));
		} else if (stats_name == "column_size_bytes") {
			D_ASSERT(!column_stats.column_size_bytes);
			column_stats.column_size_bytes = StringUtil::ToUnsigned(StringValue::Get(stats_children[1]));
		} else if (stats_name == "has_nan") {
			D_ASSERT(!column_stats.contains_nan);
			column_stats.contains_nan = StringValue::Get(stats_children[1]) == "true";
		} else if (stats_name == "variant_type") {
			//! Should be handled elsewhere
			continue;
		} else if (stats_name == "bbox_xmin") {
			column_stats.bbox_xmin = std::stod(StringValue::Get(stats_children[1]));
		} else if (stats_name == "bbox_xmax") {
			column_stats.bbox_xmax = std::stod(StringValue::Get(stats_children[1]));
		} else if (stats_name == "bbox_ymin") {
			column_stats.bbox_ymin = std::stod(StringValue::Get(stats_children[1]));
			// xmin/xmax/ymin/ymax are always emitted together by the writer; flag XY
			// once we've seen ymin to know we have at least the 2D bbox.
			column_stats.has_bbox_xy = true;
		} else if (stats_name == "bbox_ymax") {
			column_stats.bbox_ymax = std::stod(StringValue::Get(stats_children[1]));
		} else if (stats_name == "bbox_zmin") {
			column_stats.bbox_zmin = std::stod(StringValue::Get(stats_children[1]));
		} else if (stats_name == "bbox_zmax") {
			column_stats.bbox_zmax = std::stod(StringValue::Get(stats_children[1]));
			column_stats.has_bbox_z = true;
		} else if (stats_name == "bbox_mmin") {
			column_stats.bbox_mmin = std::stod(StringValue::Get(stats_children[1]));
		} else if (stats_name == "bbox_mmax") {
			column_stats.bbox_mmax = std::stod(StringValue::Get(stats_children[1]));
			column_stats.has_bbox_m = true;
		} else if (stats_name == "geo_types") {
			// TODO: Iceberg has no standard manifest field for geometry type set
			continue;
		} else {
			// Ignore other stats types
			DUCKDB_LOG_INFO(context, StringUtil::Format("Did not write column stats %s", stats_name));
		}
	}
	return column_stats;
}

} // namespace duckdb
