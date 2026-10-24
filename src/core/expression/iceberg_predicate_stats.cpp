#include "core/expression/iceberg_predicate_stats.hpp"

#include "core/expression/iceberg_value.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "iceberg_logging.hpp"

#include <limits>

namespace duckdb {

void IcebergPredicateStats::SetLowerBound(const Value &new_lower_bound) {
	lower_bound = new_lower_bound;
}

void IcebergPredicateStats::SetUpperBound(const Value &new_upper_bound) {
	upper_bound = new_upper_bound;
}

bool IcebergPredicateStats::BoundsAreNull() const {
	return lower_bound && upper_bound && lower_bound->IsNull() && upper_bound->IsNull();
}

static shared_ptr<BaseStatistics> BuildGeometryStats(const Value &lower_bound, const Value &upper_bound,
                                                     const LogicalType &type) {
	if (lower_bound.IsNull() || upper_bound.IsNull()) {
		return nullptr;
	}
	auto lower_blob = lower_bound.GetValueUnsafe<string_t>();
	auto upper_blob = upper_bound.GetValueUnsafe<string_t>();
	const auto lower_coordinate_card = lower_blob.GetSize() / sizeof(double);
	const auto upper_coordinate_card = upper_blob.GetSize() / sizeof(double);
	if (lower_coordinate_card < 2 || upper_coordinate_card < 2) {
		return nullptr;
	}
	const auto *lo = reinterpret_cast<const double *>(lower_blob.GetData());
	const auto *hi = reinterpret_cast<const double *>(upper_blob.GetData());

	auto stats = make_shared_ptr<BaseStatistics>(GeometryStats::CreateUnknown(type));
	auto &extent = GeometryStats::GetExtent(*stats);
	extent.x_min = lo[0];
	extent.y_min = lo[1];
	extent.x_max = hi[0];
	extent.y_max = hi[1];
	if (lower_coordinate_card >= 3 && upper_coordinate_card >= 3) {
		extent.z_min = lo[2];
		extent.z_max = hi[2];
	}
	if (lower_coordinate_card >= 4 && upper_coordinate_card >= 4) {
		extent.m_min = lo[3];
		extent.m_max = hi[3];
	}
	return stats;
}

shared_ptr<BaseStatistics> IcebergPredicateStats::BuildColumnStats(const LogicalType &type) const {
	switch (BaseStatistics::GetStatsType(type)) {
	case StatisticsType::NUMERIC_STATS: {
		auto stats = NumericStats::CreateEmpty(type);
		if (lower_bound && !lower_bound->IsNull()) {
			NumericStats::SetMin(stats, *lower_bound);
		}
		if (upper_bound && !upper_bound->IsNull()) {
			NumericStats::SetMax(stats, *upper_bound);
		}
		if (type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE) {
			//! Iceberg float bounds exclude NaN, which core zonemaps order as the largest value
			if (!has_nan || *has_nan) {
				NumericStats::SetMax(stats, type.id() == LogicalTypeId::FLOAT
				                                ? Value::FLOAT(std::numeric_limits<float>::quiet_NaN())
				                                : Value::DOUBLE(std::numeric_limits<double>::quiet_NaN()));
			}
		}
		if (has_null) {
			stats.SetHasNull();
		}
		if (has_not_null) {
			stats.SetHasNoNull();
		}
		return make_shared_ptr<BaseStatistics>(std::move(stats));
	}
	case StatisticsType::STRING_STATS: {
		auto stats = StringStats::CreateUnknown(type);
		//! Iceberg string bounds may be truncated
		if (lower_bound && !lower_bound->IsNull()) {
			StringStats::SetMin(stats, lower_bound->GetValueUnsafe<string_t>(), StringStatsType::TRUNCATED_STATS);
		}
		if (upper_bound && !upper_bound->IsNull()) {
			StringStats::SetMax(stats, upper_bound->GetValueUnsafe<string_t>(), StringStatsType::TRUNCATED_STATS);
		}
		stats.Set(has_null ? StatsInfo::CAN_HAVE_NULL_VALUES : StatsInfo::CANNOT_HAVE_NULL_VALUES);
		stats.Set(has_not_null ? StatsInfo::CAN_HAVE_VALID_VALUES : StatsInfo::CANNOT_HAVE_VALID_VALUES);
		return make_shared_ptr<BaseStatistics>(std::move(stats));
	}
	default:
		return nullptr;
	}
}

IcebergPredicateStats IcebergPredicateStats::DeserializeBounds(ClientContext &context, const Value &lower_bound,
                                                               const Value &upper_bound, const string &name,
                                                               const LogicalType &type) {
	IcebergPredicateStats result;
	if (type.id() == LogicalTypeId::GEOMETRY) {
		result.geometry_stats = BuildGeometryStats(lower_bound, upper_bound, type);
		if (!result.geometry_stats) {
			result.lower_bound.reset();
			result.upper_bound.reset();
		}
		return result;
	}

	if (!lower_bound.IsNull()) {
		D_ASSERT(lower_bound.type().id() == LogicalTypeId::BLOB);
		auto deserialized =
		    IcebergValue::DeserializeValue(lower_bound.GetValueUnsafe<string_t>(), type, SerializeBound::LOWER_BOUND);
		if (deserialized.HasError()) {
			// Bounds are optional, so omit one that cannot be deserialized rather than failing the scan.
			DUCKDB_LOG(context, IcebergLogType, "Omitting invalid lower bound for column '%s' of type %s", name,
			           type.ToString());
		} else {
			result.SetLowerBound(deserialized.GetValue());
		}
	}
	if (!upper_bound.IsNull()) {
		D_ASSERT(upper_bound.type().id() == LogicalTypeId::BLOB);
		auto deserialized =
		    IcebergValue::DeserializeValue(upper_bound.GetValueUnsafe<string_t>(), type, SerializeBound::UPPER_BOUND);
		if (deserialized.HasError()) {
			// Bounds are optional, so omit one that cannot be deserialized rather than failing the scan.
			DUCKDB_LOG(context, IcebergLogType, "Omitting invalid upper bound for column '%s' of type %s", name,
			           type.ToString());
		} else {
			result.SetUpperBound(deserialized.GetValue());
		}
	}
	return result;
}

} // namespace duckdb
